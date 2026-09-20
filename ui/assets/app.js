/*
 * Sub2api State Guard 配置页逻辑。
 *
 * 只能通过 UI Bridge 读写配置：没有 fetch/XHR（CSP connect-src 'none'），
 * 也没有管理员 Token。页面维护一份草稿，保存时整份交给宿主 → 插件 ValidateConfig
 * 做权威校验，回传的规范化配置再刷新界面，避免前后端校验规则漂移。
 *
 * 实时看板：打开页面先跑一次 config.test 铺满一屏（宿主对它做二次验证门控、失败时才弹提示）；
 * 之后走同一条 Bridge 的 plugin.status 动词轮询——宿主把它映射到插件 Health，只读、免二次验证、
 * 不弹宿主提示。宿主太老（< 0.2.7）不认识这个动词时停掉轮询，「刷新状态」改走手动 config.test，
 * 结构化数据搭在返回消息末尾的哨兵行里。
 */
(function () {
  "use strict";

  /* ---------- 默认值：与 internal/pluginconfig.Default() 一一对应 ---------- */

  // 宿主对从未配置过的插件会回一个空对象，这时页面显示的必须就是插件真正会用的默认值，
  // 否则管理员一保存就把「默认」改成了「页面上碰巧显示的东西」。
  var TICKET_DEFAULTS = {
    models: ["gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna", "gpt-6-astra"],
    follow_observed_models: true,
    pool_size: 5,
    target_state_length: 292,
    // 满血长度按模型不同：5.5 是 292（上面那个兜底值），5.6 / 6 系列是 312。
    model_state_lengths: {
      "gpt-5.6-sol": 312,
      "gpt-5.6-terra": 312,
      "gpt-5.6-luna": 312,
      "gpt-6-astra": 312,
    },
    ticket_ttl_seconds: 2700,
    ticket_stagger_seconds: 600,
    refill_threshold_seconds: 600,
    check_interval_seconds: 30,
    probe_timeout_seconds: 45,
    probe_effort: "low",
    proxies_per_round: 4,
    retry_rounds: 3,
    include_direct: true,
    gateway_base_url: "https://chatgpt.com/backend-api/codex",
    user_agent: "codex_cli_rs/0.154.0",
  };

  var PROXY_DEFAULTS = {
    enabled: true,
    proxies: [],
  };

  var DEFAULTS = {
    request_timeout_seconds: 0,
    response_header_timeout_seconds: 300,
    dial_timeout_seconds: 10,
    tls_handshake_timeout_seconds: 10,
    idle_connection_timeout_seconds: 90,
    max_idle_connections: 240,
    max_idle_connections_per_host: 120,
    max_connections_per_host: 240,
    enable_http2: true,
    http2_read_idle_timeout_seconds: 10,
    tls_min_version: "1.2",
    proxy_mode: "host",
    extra_headers: {},
    overload_guard: {
      enabled: true,
      header_name: "X-Codex-Turn-State",
      override_mode: "always",
      on_unavailable: "passthrough",
      on_model_unmatched: "passthrough",
      model_sniff_max_bytes: 262144,
      ticket_pool: TICKET_DEFAULTS,
      proxy_pool: PROXY_DEFAULTS,
      accounts: [],
    },
  };

  /* ---------- 限值：镜像 internal/pluginconfig/validate.go ---------- */

  var MAX_ACCOUNTS = 500;
  var MAX_POOL_MODELS = 16;
  var MAX_MODEL_STATE_LENGTHS = 32;
  var MAX_PROXY_ITEMS = 200;
  var MAX_URL_BYTES = 2048;
  var MAX_NOTE_BYTES = 120;
  var MAX_PROXY_NAME_BYTES = 40;
  var MAX_MODEL_NAME_BYTES = 120;
  var MAX_USER_AGENT_BYTES = 200;
  var MIN_SNIFF = 1024;
  var MAX_SNIFF = 1048576;

  var PROBE_EFFORTS = ["none", "minimal", "low", "medium", "high"];
  // 与 pluginconfig 的 ProxyScheme* 一一对应；默认端口同 defaultProxyPorts。
  var PROXY_SCHEMES = ["socks5", "socks5h", "http", "https"];
  var DEFAULT_PROXY_SCHEME = "socks5";
  var DEFAULT_PROXY_PORTS = { socks5: "1080", socks5h: "1080", http: "80", https: "443" };

  var MODEL_NAME_CHARS = /^[A-Za-z0-9._:/-]+$/;
  // 与宿主 httpguts.ValidHeaderFieldValue 对齐：控制字符一律不许，制表符除外。
  var CONTROL_CHARS = /[\u0000-\u0008\u000A-\u001F\u007F]/;

  /* ---------- 看板 ---------- */

  // STATUS_SENTINEL 必须与 internal/runtime.StatusSentinel 完全一致。
  var STATUS_SENTINEL = "<<<overload-guard-status>>>";

  // 看板轮询间隔。plugin.status 只读内存状态，宿主端也不做二次验证，
  // 10 秒一次足够跟上撞票节奏，又不至于让宿主日志被刷屏。
  var STATUS_POLL_MS = 10000;
  // 探测 plugin.status 是否可用时用的超时：老宿主不认识这个动词会直接丢消息，
  // 只能靠超时判定，所以不必等满默认的 30 秒。
  var STATUS_PROBE_TIMEOUT_MS = 8000;

  // 与 ticketpool 的分级语义、README §5.1 一致：overloaded = SSE error 里出现 overload；
  // weak = 非 200 但带回了 state 头。
  var GRADE_LABELS = {
    healthy: "满血",
    weak: "弱",
    mismatch: "长度不符",
    downgraded: "降级",
    partial: "半截",
    overloaded: "过载",
    blocked: "被拦",
    error: "出错",
  };

  var REASON_LABELS = {
    empty: "池空，全量补",
    partial: "未满，补差额",
    cooldown: "刚补过，冷却中",
    full: "已满",
    renew: "最新票也快过期，续一张",
  };

  var bridge = window.Sub2ApiBridge.create();

  var draft = null; // 当前草稿
  var savedJSON = ""; // 最近一次已保存配置的序列化结果，用于判断「未保存」
  var busy = false;
  var accountIndex = -1; // -1 表示新增
  var proxyIndex = -1;
  var lastHeight = 0;
  var heightTimer = 0;
  var statusPending = false;
  // statusChannel：unknown = 还没探测过（此时不读状态、「刷新状态」也禁用）；
  // status = 走 plugin.status（可轮询）；test = 宿主太老（< 0.2.7），只能手动 config.test。
  var statusChannel = "unknown";
  var statusNote = "";
  var statusTimer = 0;
  // lastPayload 是最近一次看板结构化数据。改动模型清单时用它就地叠加草稿重绘卡片组，
  // 免得等下一次轮询——运行时数据在保存生效前本来也不会变。
  var lastPayload = null;
  var modelsRerenderTimer = 0;
  // 「按模型的满血长度」textarea 里解析不了的行。收集时留在这儿，校验时报错，
  // 不静默丢弃——打错一个字就无声少一条配置是最难查的那种问题。
  var modelLengthBadLines = [];

  // 通道降级提示：宿主 < 0.2.7 不认识 plugin.status（表现为超时），只能退回手动 config.test，
  // 且不该自动轮询——宿主对 config.test 做二次验证门控，轮询会反复触发它。
  var STATUS_NOTE_OLD_HOST = "当前宿主没有只读状态通道（需要 sub2api ≥ 0.2.7），已停止自动刷新；"
    + "点「刷新状态」会走 config.test，宿主可能要求二次验证。";

  /* ---------- 通用小工具 ---------- */

  function el(id) {
    return document.getElementById(id);
  }

  function clone(value) {
    return JSON.parse(JSON.stringify(value));
  }

  function isPlainObject(value) {
    return value !== null && typeof value === "object" && !Array.isArray(value);
  }

  function pickBool(value, fallback) {
    return typeof value === "boolean" ? value : fallback;
  }

  function pickString(value, fallback) {
    return typeof value === "string" ? value : fallback;
  }

  function pickInt(value, fallback) {
    if (typeof value === "number" && isFinite(value)) return Math.round(value);
    if (typeof value === "string" && value.trim() !== "") {
      var parsed = Number(value);
      if (isFinite(parsed)) return Math.round(parsed);
    }
    return fallback;
  }

  function pickHeaders(value) {
    var out = {};
    if (!isPlainObject(value)) return out;
    Object.keys(value).forEach(function (name) {
      if (typeof value[name] === "string") out[name] = value[name];
    });
    return out;
  }

  function intFromInput(input, fallback) {
    var raw = String(input.value).trim();
    if (raw === "") return fallback;
    var parsed = Number(raw);
    if (!isFinite(parsed)) return fallback;
    return Math.round(parsed);
  }

  // byteLength 按 UTF-8 计字节：插件侧的长度上限全是字节数，中文备注按字符算会放行超限值。
  function byteLength(value) {
    var text = String(value);
    if (typeof TextEncoder === "function") return new TextEncoder().encode(text).length;
    return encodeURIComponent(text).replace(/%[0-9A-Fa-f]{2}/g, "x").length;
  }

  function node(tag, className, text) {
    var element = document.createElement(tag);
    if (className) element.className = className;
    if (text !== undefined && text !== null) element.textContent = String(text);
    return element;
  }

  function humanSeconds(value) {
    var seconds = Math.round(Number(value));
    if (!isFinite(seconds) || seconds <= 0) return "0 秒";
    if (seconds < 60) return seconds + " 秒";
    var minutes = Math.floor(seconds / 60);
    if (minutes < 60) {
      var restSeconds = seconds % 60;
      return restSeconds > 0 ? minutes + " 分 " + restSeconds + " 秒" : minutes + " 分";
    }
    var hours = Math.floor(minutes / 60);
    var restMinutes = minutes % 60;
    return restMinutes > 0 ? hours + " 小时 " + restMinutes + " 分" : hours + " 小时";
  }

  /* ---------- URL 校验：镜像 validatePlainURL / validateProxyURL ---------- */

  function urlShapeError(label, rawURL) {
    var match = /^(https?):\/\/([^/?#]*)/i.exec(rawURL);
    if (!match) return label + "：必须是 http:// 或 https:// 开头的绝对地址";
    if (match[2] === "") return label + "：必须包含主机名";
    if (match[2].indexOf("@") >= 0) return label + "：不允许内联用户名密码";
    return "";
  }

  function plainURLError(label, rawURL) {
    if (rawURL === "") return label + "：不能为空";
    if (byteLength(rawURL) > MAX_URL_BYTES) return label + "：不能超过 " + MAX_URL_BYTES + " 字节";
    if (/[{}]/.test(rawURL)) return label + "：不支持占位符";
    return urlShapeError(label, rawURL);
  }

  /* ---------- 代理地址：镜像 normalizeProxyURL / validateProxyURL ---------- */

  // splitProxyURL 把一条代理地址拆成弹窗里的四个字段。
  // 解析失败时把原文整个放进「地址」，让管理员自己看到哪里不对，而不是被悄悄吃掉。
  function splitProxyURL(rawURL) {
    var out = { scheme: DEFAULT_PROXY_SCHEME, host: "", username: "", password: "" };
    var value = String(rawURL || "").trim();
    if (value === "") return out;
    var at = value.indexOf("://");
    if (at >= 0) {
      out.scheme = value.slice(0, at).toLowerCase();
      value = value.slice(at + 3);
    }
    // 路径/查询/锚点在插件侧会被丢掉，这里同样只看 authority。
    var boundary = value.search(/[/?#]/);
    if (boundary >= 0) value = value.slice(0, boundary);
    var credential = value.lastIndexOf("@");
    if (credential >= 0) {
      var userinfo = value.slice(0, credential);
      value = value.slice(credential + 1);
      var colon = userinfo.indexOf(":");
      if (colon >= 0) {
        out.username = decodePart(userinfo.slice(0, colon));
        out.password = decodePart(userinfo.slice(colon + 1));
      } else {
        out.username = decodePart(userinfo);
      }
    }
    out.host = value;
    return out;
  }

  // decodePart 解 URL 编码；遇到残缺的 % 序列（例如密码里本来就有 %）就保留原文，
  // 不让一次解析异常把整个弹窗炸掉。
  function decodePart(value) {
    try {
      return decodeURIComponent(value);
    } catch (error) {
      return value;
    }
  }

  // joinProxyURL 是 splitProxyURL 的逆操作，规则与 Go 侧 normalizeProxyURL 对齐：
  // 补协议、补默认端口，用户名密码按 URL 编码写回。
  function joinProxyURL(parts) {
    var scheme = String(parts.scheme || "").trim().toLowerCase() || DEFAULT_PROXY_SCHEME;
    var host = String(parts.host || "").trim();
    if (host === "") return "";
    var userinfo = "";
    if (parts.username) {
      userinfo = encodeURIComponent(parts.username);
      if (parts.password) userinfo += ":" + encodeURIComponent(parts.password);
      userinfo += "@";
    }
    if (proxyPort(host) === "" && DEFAULT_PROXY_PORTS[scheme]) {
      host = host + ":" + DEFAULT_PROXY_PORTS[scheme];
    }
    return scheme + "://" + userinfo + host;
  }

  // proxyPort 取 host:port 里的端口，兼顾 IPv6 的方括号写法；没有端口返回空串。
  function proxyPort(host) {
    if (host.charAt(0) === "[") {
      var close = host.indexOf("]");
      if (close < 0) return "";
      return host.charAt(close + 1) === ":" ? host.slice(close + 2) : "";
    }
    var parts = host.split(":");
    return parts.length === 2 ? parts[1] : "";
  }

  function proxyHostname(host) {
    if (host.charAt(0) === "[") {
      var close = host.indexOf("]");
      return close < 0 ? "" : host.slice(1, close);
    }
    var parts = host.split(":");
    return parts.length <= 2 ? parts[0] : "";
  }

  function proxyURLError(label, rawURL) {
    if (rawURL === "") return label + "：不能为空";
    if (byteLength(rawURL) > MAX_URL_BYTES) return label + "：不能超过 " + MAX_URL_BYTES + " 字节";
    if (CONTROL_CHARS.test(rawURL)) return label + "：不能包含控制字符";
    var parts = splitProxyURL(rawURL);
    if (PROXY_SCHEMES.indexOf(parts.scheme) < 0) {
      return label + "：协议只支持 " + PROXY_SCHEMES.join("/");
    }
    if (proxyHostname(parts.host) === "") return label + "：缺少主机名";
    var port = proxyPort(parts.host);
    if (port === "") return label + "：缺少端口";
    if (!/^[0-9]+$/.test(port) || Number(port) < 1 || Number(port) > 65535) {
      return label + "：端口必须是 1-65535";
    }
    return "";
  }

  // isIPv4 与 Go 的 net.ParseIP 对四段点分写法的判定对齐：每段 1-3 位十进制、不带前导零、≤ 255。
  function isIPv4(host) {
    var segments = host.split(".");
    if (segments.length !== 4) return false;
    for (var index = 0; index < segments.length; index += 1) {
      var segment = segments[index];
      if (!/^[0-9]{1,3}$/.test(segment)) return false;
      if (segment.length > 1 && segment.charAt(0) === "0") return false;
      if (Number(segment) > 255) return false;
    }
    return true;
  }

  // maskProxyURL 只在界面上展示打码后的地址，与 proxypool.MaskProxy 同一套规则：
  // 不显示用户名密码，IPv4 只留前两段，其它主机名只留前两个字符；IPv6 打码后仍带方括号
  // （Go 侧用 net.JoinHostPort 拼回去）。配置页本身不是秘密，但截图与录屏很常见。
  // 返回的是 host:port 部分，不带协议——代理表另有「协议」一列。
  function maskProxyURL(rawURL) {
    var parts = splitProxyURL(rawURL);
    var host = proxyHostname(parts.host);
    var port = proxyPort(parts.host);
    var masked;
    if (isIPv4(host)) {
      var segments = host.split(".");
      masked = segments[0] + "." + segments[1] + ".*.*";
    } else if (host.length <= 2) {
      masked = host + "***";
    } else {
      masked = host.slice(0, 2) + "***";
    }
    if (masked.indexOf(":") >= 0) masked = "[" + masked + "]";
    return masked + (port ? ":" + port : "");
  }

  // maskProxyAddr 复现 Go 侧 proxypool.MaskProxy 的完整输出（scheme://打码地址），
  // 看板里每条代理的 addr 就是它，代理表的「状态」列靠这个键去对上使用统计。
  function maskProxyAddr(rawURL) {
    var value = String(rawURL || "").trim();
    if (value === "") return "***";
    var parts = splitProxyURL(value);
    if (parts.host === "") return "***";
    return parts.scheme + "://" + maskProxyURL(value);
  }

  /* ---------- 配置归一化（宿主可能返回 {} 或旧版本配置） ---------- */

  function normalizeModels(raw) {
    var list = Array.isArray(raw) ? raw : [];
    var seen = {};
    var out = [];
    list.forEach(function (item) {
      if (typeof item !== "string") return;
      var trimmed = item.trim();
      if (trimmed === "" || Object.prototype.hasOwnProperty.call(seen, trimmed)) return;
      seen[trimmed] = true;
      out.push(trimmed);
    });
    return out;
  }

  // normalizeModelLengths 把「按模型的满血长度」收敛成一个干净的 {模型: 长度} 对象。
  // 与 Go 侧 Ticket.normalize 一致：键去空格、丢空键、负值归 0。
  function normalizeModelLengths(raw) {
    var source = isPlainObject(raw) ? raw : {};
    var out = {};
    Object.keys(source).forEach(function (key) {
      var model = String(key).trim();
      if (model === "") return;
      var length = Math.trunc(Number(source[key]));
      if (!isFinite(length) || length < 0) length = 0;
      out[model] = length;
    });
    return out;
  }

  // modelLengthsToText / textToModelLengths 在对象与「一行一个 模型名=长度」之间互转。
  // 键排序后输出，这样光是打开页面不会让草稿看起来"改过"。
  function modelLengthsToText(lengths) {
    return Object.keys(lengths)
      .sort()
      .map(function (model) {
        return model + "=" + lengths[model];
      })
      .join("\n");
  }

  // 解析失败的行（没有 = 、长度不是数字）原样留在 invalid 里交给校验报错，
  // 不静默丢弃——否则管理员打错一个字，那条配置就无声消失了。
  function textToModelLengths(text) {
    var out = {};
    var invalid = [];
    String(text)
      .split(/\r?\n/)
      .forEach(function (line) {
        var trimmed = line.trim();
        if (trimmed === "") return;
        var at = trimmed.lastIndexOf("=");
        if (at < 0) {
          invalid.push(trimmed);
          return;
        }
        var model = trimmed.slice(0, at).trim();
        var raw = trimmed.slice(at + 1).trim();
        if (model === "" || !/^\d+$/.test(raw)) {
          invalid.push(trimmed);
          return;
        }
        out[model] = parseInt(raw, 10);
      });
    return { lengths: out, invalid: invalid };
  }

  function normalizeTicket(raw) {
    var source = isPlainObject(raw) ? raw : {};
    var models = source.models === undefined ? TICKET_DEFAULTS.models : source.models;
    var lengths =
      source.model_state_lengths === undefined
        ? TICKET_DEFAULTS.model_state_lengths
        : source.model_state_lengths;
    var effort = pickString(source.probe_effort, TICKET_DEFAULTS.probe_effort).trim().toLowerCase();
    return {
      models: normalizeModels(models),
      follow_observed_models: pickBool(source.follow_observed_models, TICKET_DEFAULTS.follow_observed_models),
      pool_size: pickInt(source.pool_size, TICKET_DEFAULTS.pool_size),
      target_state_length: pickInt(source.target_state_length, TICKET_DEFAULTS.target_state_length),
      model_state_lengths: normalizeModelLengths(lengths),
      ticket_ttl_seconds: pickInt(source.ticket_ttl_seconds, TICKET_DEFAULTS.ticket_ttl_seconds),
      ticket_stagger_seconds: pickInt(source.ticket_stagger_seconds, TICKET_DEFAULTS.ticket_stagger_seconds),
      refill_threshold_seconds: pickInt(source.refill_threshold_seconds, TICKET_DEFAULTS.refill_threshold_seconds),
      check_interval_seconds: pickInt(source.check_interval_seconds, TICKET_DEFAULTS.check_interval_seconds),
      probe_timeout_seconds: pickInt(source.probe_timeout_seconds, TICKET_DEFAULTS.probe_timeout_seconds),
      probe_effort: PROBE_EFFORTS.indexOf(effort) >= 0 ? effort : TICKET_DEFAULTS.probe_effort,
      proxies_per_round: pickInt(source.proxies_per_round, TICKET_DEFAULTS.proxies_per_round),
      retry_rounds: pickInt(source.retry_rounds, TICKET_DEFAULTS.retry_rounds),
      include_direct: pickBool(source.include_direct, TICKET_DEFAULTS.include_direct),
      gateway_base_url: pickString(source.gateway_base_url, TICKET_DEFAULTS.gateway_base_url).trim()
        .replace(/\/+$/, "") || TICKET_DEFAULTS.gateway_base_url,
      user_agent: pickString(source.user_agent, TICKET_DEFAULTS.user_agent).trim() || TICKET_DEFAULTS.user_agent,
    };
  }

  // normalizeProxyItem 与 Go 侧 normalizeProxyURL 同规则：补协议、补默认端口、丢路径。
  function normalizeProxyItem(raw) {
    var source = isPlainObject(raw) ? raw : {};
    var url = pickString(source.url, "").trim();
    return {
      name: pickString(source.name, "").trim(),
      url: url === "" ? "" : joinProxyURL(splitProxyURL(url)),
      enabled: pickBool(source.enabled, true),
    };
  }

  function normalizeProxy(raw) {
    var source = isPlainObject(raw) ? raw : {};
    var items = Array.isArray(source.proxies) ? source.proxies : [];
    return {
      enabled: pickBool(source.enabled, PROXY_DEFAULTS.enabled),
      proxies: items.map(normalizeProxyItem),
    };
  }

  // normalizeConfig 只保留当前 schema 里的字段：旧配置里的 default_provider / model_aliases /
  // 账号级 provider 与 provider_account_id、以及代理池早期的接口拉取源（sources/ttl_seconds/
  // max_size 等）在这里被丢掉，保存一次即清干净。
  // 插件侧对这些键是「接受并忽略」，所以不清也不会报错，但留着只会让人以为它们还有用。
  function normalizeConfig(raw) {
    var source = isPlainObject(raw) ? raw : {};
    var guardSource = isPlainObject(source.overload_guard) ? source.overload_guard : {};
    var accounts = Array.isArray(guardSource.accounts) ? guardSource.accounts : [];
    var guardDefaults = DEFAULTS.overload_guard;

    return {
      request_timeout_seconds: pickInt(source.request_timeout_seconds, DEFAULTS.request_timeout_seconds),
      response_header_timeout_seconds: pickInt(
        source.response_header_timeout_seconds,
        DEFAULTS.response_header_timeout_seconds,
      ),
      dial_timeout_seconds: pickInt(source.dial_timeout_seconds, DEFAULTS.dial_timeout_seconds),
      tls_handshake_timeout_seconds: pickInt(
        source.tls_handshake_timeout_seconds,
        DEFAULTS.tls_handshake_timeout_seconds,
      ),
      idle_connection_timeout_seconds: pickInt(
        source.idle_connection_timeout_seconds,
        DEFAULTS.idle_connection_timeout_seconds,
      ),
      max_idle_connections: pickInt(source.max_idle_connections, DEFAULTS.max_idle_connections),
      max_idle_connections_per_host: pickInt(
        source.max_idle_connections_per_host,
        DEFAULTS.max_idle_connections_per_host,
      ),
      max_connections_per_host: pickInt(source.max_connections_per_host, DEFAULTS.max_connections_per_host),
      enable_http2: pickBool(source.enable_http2, DEFAULTS.enable_http2),
      http2_read_idle_timeout_seconds: pickInt(
        source.http2_read_idle_timeout_seconds,
        DEFAULTS.http2_read_idle_timeout_seconds,
      ),
      tls_min_version: pickString(source.tls_min_version, DEFAULTS.tls_min_version) === "1.3" ? "1.3" : "1.2",
      proxy_mode: pickString(source.proxy_mode, DEFAULTS.proxy_mode) === "disabled" ? "disabled" : "host",
      extra_headers: pickHeaders(source.extra_headers),
      overload_guard: {
        enabled: pickBool(guardSource.enabled, guardDefaults.enabled),
        header_name: pickString(guardSource.header_name, guardDefaults.header_name) || guardDefaults.header_name,
        override_mode:
          pickString(guardSource.override_mode, guardDefaults.override_mode) === "fill_missing"
            ? "fill_missing"
            : "always",
        on_unavailable:
          pickString(guardSource.on_unavailable, guardDefaults.on_unavailable) === "fail_request"
            ? "fail_request"
            : "passthrough",
        on_model_unmatched:
          pickString(guardSource.on_model_unmatched, guardDefaults.on_model_unmatched) === "fail_request"
            ? "fail_request"
            : "passthrough",
        model_sniff_max_bytes: pickInt(guardSource.model_sniff_max_bytes, guardDefaults.model_sniff_max_bytes),
        ticket_pool: normalizeTicket(guardSource.ticket_pool),
        proxy_pool: normalizeProxy(guardSource.proxy_pool),
        accounts: accounts.map(function (item) {
          var entry = isPlainObject(item) ? item : {};
          return {
            account_id: pickInt(entry.account_id, 0),
            enabled: pickBool(entry.enabled, false),
            note: pickString(entry.note, "").trim(),
          };
        }),
      },
    };
  }

  /* ---------- 表单绑定 ---------- */

  // 下面几张表把输入框 id 与草稿字段配对，写入与读回共用它们，避免两边漏改。
  var TICKET_NUMBERS = [
    ["tp-pool-size", "pool_size"],
    ["tp-target-length", "target_state_length"],
    ["tp-ttl", "ticket_ttl_seconds"],
    ["tp-stagger", "ticket_stagger_seconds"],
    ["tp-threshold", "refill_threshold_seconds"],
    ["tp-interval", "check_interval_seconds"],
    ["tp-probe-timeout", "probe_timeout_seconds"],
    ["tp-proxies-per-round", "proxies_per_round"],
    ["tp-retry-rounds", "retry_rounds"],
  ];
  var TICKET_TEXTS = [
    ["tp-gateway", "gateway_base_url"],
    ["tp-user-agent", "user_agent"],
  ];
  var TICKET_FLAGS = [
    ["tp-follow", "follow_observed_models"],
    ["tp-include-direct", "include_direct"],
  ];
  var PROXY_FLAGS = [["pp-enabled", "enabled"]];

  function writeSection(pairs, values, kind) {
    pairs.forEach(function (pair) {
      var input = el(pair[0]);
      if (kind === "flag") input.checked = values[pair[1]];
      else input.value = String(values[pair[1]]);
    });
  }

  // readSection 的回退值取「默认值」而不是草稿现值：清空一个数字框的意思就是「恢复默认」，
  // 保存后页面会用插件回传的规范化配置重绘，所见即所得。
  function readSection(pairs, target, defaults, kind) {
    pairs.forEach(function (pair) {
      var input = el(pair[0]);
      if (kind === "flag") target[pair[1]] = input.checked;
      else if (kind === "text") target[pair[1]] = input.value.trim() || defaults[pair[1]];
      else target[pair[1]] = intFromInput(input, defaults[pair[1]]);
    });
  }

  // collectDraft 把页面上展示的项写回草稿。账号与代理列表只由弹窗和行内按钮改动，不在这里读。
  function collectDraft() {
    var next = clone(draft);
    var guard = next.overload_guard;
    guard.enabled = el("guard-enabled").checked;

    var ticket = guard.ticket_pool;
    ticket.models = normalizeModels(el("tp-models").value.split(/\r?\n/));
    var parsed = textToModelLengths(el("tp-model-lengths").value);
    ticket.model_state_lengths = parsed.lengths;
    modelLengthBadLines = parsed.invalid;
    readSection(TICKET_NUMBERS, ticket, TICKET_DEFAULTS, "number");
    readSection(TICKET_TEXTS, ticket, TICKET_DEFAULTS, "text");
    readSection(TICKET_FLAGS, ticket, TICKET_DEFAULTS, "flag");
    ticket.probe_effort = el("tp-effort").value;
    ticket.gateway_base_url = ticket.gateway_base_url.replace(/\/+$/, "") || TICKET_DEFAULTS.gateway_base_url;

    readSection(PROXY_FLAGS, guard.proxy_pool, PROXY_DEFAULTS, "flag");
    return next;
  }

  function syncDraft() {
    draft = collectDraft();
    renderHead();
  }

  function renderForm() {
    var guard = draft.overload_guard;
    el("guard-enabled").checked = guard.enabled;

    el("tp-models").value = guard.ticket_pool.models.join("\n");
    el("tp-model-lengths").value = modelLengthsToText(guard.ticket_pool.model_state_lengths);
    modelLengthBadLines = [];
    writeSection(TICKET_NUMBERS, guard.ticket_pool, "number");
    writeSection(TICKET_TEXTS, guard.ticket_pool, "text");
    writeSection(TICKET_FLAGS, guard.ticket_pool, "flag");
    el("tp-effort").value = guard.ticket_pool.probe_effort;

    writeSection(PROXY_FLAGS, guard.proxy_pool, "flag");

    renderProxies();
    renderAccounts();
    renderHead();
  }

  /* ---------- 列表渲染 ---------- */

  function textCell(row, text, className) {
    var cell = node("td", className, text);
    row.appendChild(cell);
    return cell;
  }

  function tagCell(row, text, css) {
    var cell = node("td");
    cell.appendChild(node("span", "tag " + css, text));
    row.appendChild(cell);
    return cell;
  }

  function actionButton(label, handler, extraClass) {
    var button = node("button", "btn btn-sm" + (extraClass ? " " + extraClass : ""), label);
    button.type = "button";
    button.addEventListener("click", handler);
    return button;
  }

  function actionCell(row, buttons) {
    var cell = node("td", "col-actions");
    var group = node("div", "row-actions");
    buttons.forEach(function (button) {
      group.appendChild(button);
    });
    cell.appendChild(group);
    row.appendChild(cell);
  }

  function accountStatus(account) {
    if (!account.enabled) return { text: "未开启", css: "tag-off" };
    if (!draft.overload_guard.enabled) return { text: "总开关已关闭", css: "tag-warn" };
    return { text: "已开启", css: "tag-on" };
  }

  function renderAccounts() {
    var tbody = el("account-rows");
    tbody.textContent = "";
    var accounts = draft.overload_guard.accounts;
    el("account-empty").hidden = accounts.length > 0;

    accounts.forEach(function (account, index) {
      var row = node("tr");
      textCell(row, String(account.account_id));
      textCell(row, account.note || "—", "note");
      var status = accountStatus(account);
      tagCell(row, status.text, status.css);
      actionCell(row, [
        actionButton(
          account.enabled ? "关闭" : "开启",
          function () {
            var entry = draft.overload_guard.accounts[index];
            entry.enabled = !entry.enabled;
            renderAccounts();
            renderHead();
          },
          account.enabled ? "btn-ghost" : "btn-primary",
        ),
        actionButton("配置", function () {
          openAccountModal(index);
        }),
        actionButton(
          "删除",
          function () {
            draft.overload_guard.accounts.splice(index, 1);
            renderAccounts();
            renderHead();
          },
          "btn-ghost",
        ),
      ]);
      tbody.appendChild(row);
    });
  }

  // proxyUsage 是最近一次看板返回的每条代理使用情况，键就是 Go 侧已打码的 addr
  // （scheme://打码地址，见 proxypool.MaskProxy）。plugin.status 与 config.test 带回的是同一份
  // proxies.entries；打开页面、还没读到任何一次看板之前它是空的。
  var proxyUsage = {};

  // formatClock 把 Go 的 time.Time（RFC 3339）格式成本地 HH:MM:SS；零值或解析失败返回空串。
  function formatClock(value) {
    if (typeof value !== "string" || value === "") return "";
    var time = Date.parse(value);
    // Go 的零值时间序列化成 0001-01-01T00:00:00Z，解析出来是个负数，同样当作「没有」。
    if (!isFinite(time) || time <= 0) return "";
    var date = new Date(time);
    function pad(number) {
      return (number < 10 ? "0" : "") + number;
    }
    return pad(date.getHours()) + ":" + pad(date.getMinutes()) + ":" + pad(date.getSeconds());
  }

  // usageText 把一条代理的使用统计写成一句：成功 N · 失败 M[ · 最近成功 HH:MM:SS]。
  function usageText(usage) {
    var text = "成功 " + (usage.success || 0) + " · 失败 " + (usage.fail || 0);
    var lastOK = formatClock(usage.last_ok);
    if (lastOK) text += " · 最近成功 " + lastOK;
    return text;
  }

  function proxyStatusCell(row, item) {
    if (!item.enabled) {
      tagCell(row, "未启用", "tag-off");
      return;
    }
    if (!draft.overload_guard.proxy_pool.enabled) {
      tagCell(row, "代理池已关闭", "tag-warn");
      return;
    }
    var usage = proxyUsage[maskProxyAddr(item.url)];
    if (!usage || usage.success + usage.fail === 0) {
      tagCell(row, "未使用", "tag-on");
      return;
    }
    textCell(row, usageText(usage));
  }

  function renderProxies() {
    var tbody = el("proxy-rows");
    tbody.textContent = "";
    var proxies = draft.overload_guard.proxy_pool.proxies;
    el("proxy-empty").hidden = proxies.length > 0;

    proxies.forEach(function (item, index) {
      var parts = splitProxyURL(item.url);
      var row = node("tr");
      textCell(row, item.name || "—", "note");
      textCell(row, parts.scheme);
      // 地址列打码显示：配置页经常被截图，完整地址与凭据没必要摊在上面。
      textCell(row, maskProxyURL(item.url) || "—", "url");
      proxyStatusCell(row, item);
      actionCell(row, [
        actionButton(
          item.enabled ? "停用" : "启用",
          function () {
            var entry = draft.overload_guard.proxy_pool.proxies[index];
            entry.enabled = !entry.enabled;
            renderProxies();
            renderHead();
          },
          item.enabled ? "btn-ghost" : "btn-primary",
        ),
        actionButton("编辑", function () {
          openProxyModal(index);
        }),
        actionButton(
          "删除",
          function () {
            draft.overload_guard.proxy_pool.proxies.splice(index, 1);
            renderProxies();
            renderHead();
          },
          "btn-ghost",
        ),
      ]);
      tbody.appendChild(row);
    });
  }

  function renderHead() {
    var guard = draft.overload_guard;
    var active = guard.accounts.filter(function (account) {
      return accountStatus(account).css === "tag-on";
    }).length;
    el("head-status").textContent = guard.enabled
      ? "接管 " + active + " / " + guard.accounts.length + " 个账号"
      : "总开关已关闭";
    el("dirty").hidden = JSON.stringify(draft) === savedJSON;
    reportHeight();
  }

  function showDiagnostic(kind, text) {
    var box = el("diagnostic");
    box.className = "diagnostic" + (kind ? " " + kind : "");
    box.textContent = text;
    box.hidden = text === "";
    reportHeight();
  }

  function setBusy(value) {
    busy = value;
    el("btn-save").disabled = value;
    el("btn-reload").disabled = value;
  }

  function fatal(message) {
    var box = el("fatal");
    box.textContent = message;
    box.hidden = false;
    reportHeight();
  }

  /* ---------- 客户端预校验（权威校验仍在插件侧） ---------- */

  function checkRange(label, value, minimum, maximum, errors) {
    if (!isFinite(value) || value < minimum || value > maximum) {
      errors.push(label + " 必须在 " + minimum + "-" + maximum + " 之间，当前 " + value);
    }
  }

  // validateModelLengths 与 Go 侧 validateModelStateLengths 同规则：条数上限、
  // key 走模型名那套字符集与字节数、value 为 0（不按长度判定）或 1-4096。
  function validateModelLengths(lengths, errors) {
    modelLengthBadLines.forEach(function (line) {
      errors.push("按模型的满血长度：无法解析「" + line + "」，格式是 模型名=长度");
    });
    var models = Object.keys(lengths).sort();
    if (models.length > MAX_MODEL_STATE_LENGTHS) {
      errors.push(
        "按模型的满血长度最多 " + MAX_MODEL_STATE_LENGTHS + " 条，当前 " + models.length + " 条"
      );
    }
    models.forEach(function (model) {
      if (byteLength(model) > MAX_MODEL_NAME_BYTES) {
        errors.push("模型名「" + model + "」超过 " + MAX_MODEL_NAME_BYTES + " 字节");
      } else if (!MODEL_NAME_CHARS.test(model)) {
        errors.push("模型名「" + model + "」含非法字符，只能包含字母、数字与 - _ . : /");
      }
      if (lengths[model] !== 0) {
        checkRange(model + " 的满血长度", lengths[model], 1, 4096, errors);
      }
    });
  }

  function validateTicket(ticket, errors) {
    if (ticket.models.length > MAX_POOL_MODELS) {
      errors.push("模型最多 " + MAX_POOL_MODELS + " 个，当前 " + ticket.models.length + " 个");
    }
    ticket.models.forEach(function (model) {
      if (byteLength(model) > MAX_MODEL_NAME_BYTES) {
        errors.push("模型名「" + model + "」超过 " + MAX_MODEL_NAME_BYTES + " 字节");
      } else if (!MODEL_NAME_CHARS.test(model)) {
        errors.push("模型名「" + model + "」含非法字符，只能包含字母、数字与 - _ . : /");
      }
    });
    if (ticket.models.length === 0 && !ticket.follow_observed_models) {
      errors.push("模型列表为空时必须开启「自动跟踪请求里出现的新模型」，否则不会为任何模型维护票池");
    }
    checkRange("每个池的票数", ticket.pool_size, 1, 20, errors);
    if (ticket.target_state_length !== 0) {
      checkRange("默认满血票长度", ticket.target_state_length, 1, 4096, errors);
    }
    validateModelLengths(ticket.model_state_lengths, errors);
    checkRange("票有效期", ticket.ticket_ttl_seconds, 300, 86400, errors);
    checkRange("同批错峰", ticket.ticket_stagger_seconds, 0, 3600, errors);
    checkRange("补池阈值", ticket.refill_threshold_seconds, 30, ticket.ticket_ttl_seconds, errors);
    checkRange("检查间隔", ticket.check_interval_seconds, 10, 3600, errors);
    checkRange("探针超时", ticket.probe_timeout_seconds, 5, 300, errors);
    if (PROBE_EFFORTS.indexOf(ticket.probe_effort) < 0) {
      errors.push("探针推理强度只支持 " + PROBE_EFFORTS.join("/"));
    }
    checkRange("每轮并用代理数", ticket.proxies_per_round, 0, 32, errors);
    checkRange("失败重试轮数", ticket.retry_rounds, 0, 10, errors);
    var gatewayError = plainURLError("网关地址", ticket.gateway_base_url);
    if (gatewayError !== "") errors.push(gatewayError);
    if (byteLength(ticket.user_agent) > MAX_USER_AGENT_BYTES) {
      errors.push("探针 User-Agent 不能超过 " + MAX_USER_AGENT_BYTES + " 字节");
    }
    if (CONTROL_CHARS.test(ticket.user_agent)) errors.push("探针 User-Agent 不能包含控制字符");
  }

  // validateProxyItem 单独成函数：弹窗在「确定」时就要给出同样的判定，不能等到保存。
  function validateProxyItem(item, label) {
    var errors = [];
    if (byteLength(item.name) > MAX_PROXY_NAME_BYTES) {
      errors.push(label + "：备注不能超过 " + MAX_PROXY_NAME_BYTES + " 字节");
    } else if (CONTROL_CHARS.test(item.name)) {
      errors.push(label + "：备注不能包含控制字符");
    }
    var urlError = proxyURLError(label, item.url);
    if (urlError !== "") errors.push(urlError);
    return errors;
  }

  function validateProxy(proxy, errors) {
    if (proxy.proxies.length > MAX_PROXY_ITEMS) {
      errors.push("代理最多 " + MAX_PROXY_ITEMS + " 条，当前 " + proxy.proxies.length + " 条");
    }
    var seen = {};
    proxy.proxies.forEach(function (item, index) {
      var label = "代理 " + (item.name || "#" + (index + 1));
      validateProxyItem(item, label).forEach(function (message) {
        errors.push(message);
      });
      if (item.url === "") return;
      if (Object.prototype.hasOwnProperty.call(seen, item.url)) {
        errors.push(label + "：与前面的代理重复");
        return;
      }
      seen[item.url] = true;
    });
  }

  function validateDraft(config) {
    var errors = [];
    var guard = config.overload_guard;
    if (guard.header_name === "") errors.push("注入的头名称不能为空");
    if (
      config.request_timeout_seconds !== 0 &&
      (config.request_timeout_seconds < 60 || config.request_timeout_seconds > 86400)
    ) {
      errors.push("整体请求超时必须为 0（不限制）或 60-86400 秒");
    }
    if (guard.model_sniff_max_bytes < MIN_SNIFF || guard.model_sniff_max_bytes > MAX_SNIFF) {
      errors.push("模型嗅探上限必须在 " + MIN_SNIFF + "-" + MAX_SNIFF + " 字节之间");
    }
    validateTicket(guard.ticket_pool, errors);
    validateProxy(guard.proxy_pool, errors);

    var seen = {};
    var hasEnabledAccount = false;
    guard.accounts.forEach(function (account) {
      if (account.account_id <= 0) {
        errors.push("账号 ID 必须是正整数");
        return;
      }
      if (seen[account.account_id]) {
        errors.push("账号 " + account.account_id + " 重复");
        return;
      }
      seen[account.account_id] = true;
      if (account.enabled) hasEnabledAccount = true;
      if (byteLength(account.note) > MAX_NOTE_BYTES) {
        errors.push("账号 " + account.account_id + " 的备注不能超过 " + MAX_NOTE_BYTES + " 字节");
      }
      if (CONTROL_CHARS.test(account.note)) {
        errors.push("账号 " + account.account_id + " 的备注不能包含控制字符");
      }
    });
    if (guard.accounts.length > MAX_ACCOUNTS) errors.push("账号最多 " + MAX_ACCOUNTS + " 条");

    // 既不允许直连、又没有任何启用的代理，撞票永远发不出去——插件侧同样会拦，
    // 这里提前报是为了让管理员在原地就看到原因，而不是盯着一个永远空着的票池排查。
    var activeProxies = guard.proxy_pool.enabled
      ? guard.proxy_pool.proxies.filter(function (item) {
          return item.enabled;
        }).length
      : 0;
    if (guard.enabled && hasEnabledAccount && !guard.ticket_pool.include_direct && activeProxies === 0) {
      errors.push("已开启过载防护的账号无法撞票：「每轮也用直连打一发」已关闭，且没有启用任何代理");
    }
    return errors;
  }

  /* ---------- 弹窗 ---------- */

  function anyModalOpen() {
    return !el("account-modal").hidden || !el("proxy-modal").hidden;
  }

  function openModal(id) {
    el(id).hidden = false;
    // 弹窗是 fixed 定位，撑不开文档高度，先向宿主申请最大可用高度。
    bridge.resize(960);
  }

  function closeModal(id) {
    el(id).hidden = true;
    accountIndex = -1;
    proxyIndex = -1;
    lastHeight = 0;
    reportHeight();
  }

  function modalError(id, messages) {
    var box = el(id);
    if (!messages || messages.length === 0) {
      box.hidden = true;
      return false;
    }
    box.textContent = messages.join("\n");
    box.hidden = false;
    return true;
  }

  function openAccountModal(index) {
    accountIndex = typeof index === "number" ? index : -1;
    var account =
      accountIndex >= 0
        ? clone(draft.overload_guard.accounts[accountIndex])
        : { account_id: 0, enabled: true, note: "" };

    el("account-modal-title").textContent =
      accountIndex >= 0 ? "配置账号 " + account.account_id : "添加账号";
    el("modal-account-id").value = account.account_id > 0 ? String(account.account_id) : "";
    el("modal-account-id").disabled = accountIndex >= 0;
    el("modal-note").value = account.note;
    modalError("account-modal-error", null);
    openModal("account-modal");
    el("modal-account-id").focus();
  }

  function confirmAccountModal() {
    var accountID = intFromInput(el("modal-account-id"), 0);
    var accounts = draft.overload_guard.accounts;
    if (accountID <= 0) {
      modalError("account-modal-error", ["请填写正确的账号 ID（正整数）"]);
      return;
    }
    for (var index = 0; index < accounts.length; index += 1) {
      if (index !== accountIndex && accounts[index].account_id === accountID) {
        modalError("account-modal-error", ["账号 " + accountID + " 已经在列表里了"]);
        return;
      }
    }
    if (accountIndex < 0 && accounts.length >= MAX_ACCOUNTS) {
      modalError("account-modal-error", ["账号最多 " + MAX_ACCOUNTS + " 条"]);
      return;
    }

    var note = el("modal-note").value.trim();
    if (byteLength(note) > MAX_NOTE_BYTES) {
      modalError("account-modal-error", ["备注不能超过 " + MAX_NOTE_BYTES + " 字节"]);
      return;
    }
    // 开关不在弹窗里：编辑时沿用原值，新账号默认开启。
    var enabled = accountIndex >= 0 ? accounts[accountIndex].enabled : true;
    var entry = { account_id: accountID, enabled: enabled, note: note };
    if (accountIndex >= 0) accounts[accountIndex] = entry;
    else accounts.push(entry);
    accounts.sort(function (left, right) {
      return left.account_id - right.account_id;
    });

    closeModal("account-modal");
    renderAccounts();
    renderHead();
    showDiagnostic("", "账号改动已写入草稿，记得点「保存」。");
  }

  function openProxyModal(index) {
    proxyIndex = typeof index === "number" ? index : -1;
    var item =
      proxyIndex >= 0
        ? clone(draft.overload_guard.proxy_pool.proxies[proxyIndex])
        : { name: "", url: "", enabled: true };
    var parts = splitProxyURL(item.url);

    el("proxy-modal-title").textContent = proxyIndex >= 0 ? "编辑代理" : "添加代理";
    el("proxy-name").value = item.name;
    el("proxy-scheme").value = PROXY_SCHEMES.indexOf(parts.scheme) >= 0 ? parts.scheme : DEFAULT_PROXY_SCHEME;
    el("proxy-addr").value = parts.host;
    el("proxy-username").value = parts.username;
    el("proxy-password").value = parts.password;
    el("proxy-enabled").checked = item.enabled;
    modalError("proxy-modal-error", null);
    openModal("proxy-modal");
    el("proxy-addr").focus();
  }

  // syncProxyAddr 支持整条粘贴：在地址框里贴 socks5://user:pass@h:p 就自动拆进各字段，
  // 省得管理员手工拆一遍——代理清单通常就是一行行完整 URL 发过来的。
  function syncProxyAddr() {
    var raw = el("proxy-addr").value;
    var hasScheme = raw.indexOf("://") >= 0;
    if (!hasScheme && raw.indexOf("@") < 0) return;
    var parts = splitProxyURL(raw);
    // 协议不认识（例如 ftp://）就原样留在地址框里，交给「确定」时的校验报「协议只支持 …」，
    // 而不是悄悄剥掉协议、把剩下的当成 socks5 地址收进去。
    if (hasScheme && PROXY_SCHEMES.indexOf(parts.scheme) < 0) return;
    if (hasScheme) el("proxy-scheme").value = parts.scheme;
    el("proxy-addr").value = parts.host;
    if (parts.username !== "") el("proxy-username").value = parts.username;
    if (parts.password !== "") el("proxy-password").value = parts.password;
  }

  function confirmProxyModal() {
    var proxies = draft.overload_guard.proxy_pool.proxies;
    if (proxyIndex < 0 && proxies.length >= MAX_PROXY_ITEMS) {
      modalError("proxy-modal-error", ["代理最多 " + MAX_PROXY_ITEMS + " 条"]);
      return;
    }
    var address = el("proxy-addr").value.trim();
    // 地址框里还带着协议，说明 syncProxyAddr 没认出它：整条原样交给校验，让它报出协议错误。
    var url = address.indexOf("://") >= 0
      ? address
      : joinProxyURL({
          scheme: el("proxy-scheme").value,
          host: address,
          username: el("proxy-username").value.trim(),
          password: el("proxy-password").value,
        });
    var entry = normalizeProxyItem({
      name: el("proxy-name").value,
      url: url,
      enabled: el("proxy-enabled").checked,
    });
    var errors = validateProxyItem(entry, "该代理");
    for (var index = 0; index < proxies.length; index += 1) {
      if (index !== proxyIndex && proxies[index].url === entry.url) {
        errors.push("这条代理已经在列表里了");
        break;
      }
    }
    if (modalError("proxy-modal-error", errors)) return;

    if (proxyIndex >= 0) proxies[proxyIndex] = entry;
    else proxies.push(entry);

    closeModal("proxy-modal");
    renderProxies();
    renderHead();
    showDiagnostic("", "代理改动已写入草稿，记得点「保存」。");
  }

  /* ---------- 实时看板 ---------- */

  // splitStatus 把哨兵行从展示文本里摘出来：前面是给人看的几行，最后一行是结构化数据。
  function splitStatus(message) {
    var lines = String(message || "").split("\n");
    var payload = null;
    var kept = [];
    lines.forEach(function (line) {
      if (line.indexOf(STATUS_SENTINEL) === 0) {
        try {
          payload = JSON.parse(line.slice(STATUS_SENTINEL.length));
        } catch (error) {
          payload = null;
        }
        return;
      }
      kept.push(line);
    });
    return { text: kept.join("\n").trim(), payload: payload };
  }

  function parseStatusJSON(value) {
    if (typeof value !== "string" || value === "") return null;
    try {
      var payload = JSON.parse(value);
      return isPlainObject(payload) ? payload : null;
    } catch (error) {
      return null;
    }
  }

  // viewFromStatus 归一化 plugin.status 的返回（宿主直接转发插件 Health）。
  function viewFromStatus(result) {
    return {
      success: result.healthy !== false,
      text: String(result.message || ""),
      payload: parseStatusJSON(result.status_json),
    };
  }

  // viewFromTest 归一化 config.test 的返回：优先读 status_json，
  // 没有这个字段的老宿主就从消息末尾的哨兵行里取。
  function viewFromTest(result) {
    var parsed = splitStatus(result.message);
    return {
      success: result.success !== false,
      text: parsed.text,
      payload: parseStatusJSON(result.status_json) || parsed.payload,
    };
  }

  function pill(container, label, value, css) {
    var box = node("div", "pill" + (css ? " " + css : ""));
    box.appendChild(node("span", "pill-label", label));
    box.appendChild(node("span", "pill-value", value));
    container.appendChild(box);
  }

  function renderPills(payload) {
    var container = el("status-pills");
    container.textContent = "";
    var guard = payload.guard || {};
    var tickets = payload.tickets || {};
    var proxies = payload.proxies || {};
    var accounts = Array.isArray(tickets.accounts) ? tickets.accounts : [];
    var withCredential = accounts.filter(function (account) {
      return account.has_credential;
    }).length;

    if (!guard.enabled) {
      pill(container, "过载防护", "已关闭", "pill-off");
    } else {
      pill(
        container,
        "票池",
        (tickets.valid || 0) + " / " + (tickets.wanted || 0) + " 张",
        (tickets.valid || 0) > 0 ? "pill-ok" : "pill-warn",
      );
    }
    pill(
      container,
      "账号",
      accounts.length + " 个（" + withCredential + " 个已取到凭据）",
      accounts.length > 0 && withCredential === accounts.length ? "pill-ok" : "",
    );
    if (proxies.enabled) {
      pill(
        container,
        "代理库",
        (proxies.total || 0) + " 条",
        (proxies.total || 0) > 0 ? "pill-ok" : "pill-warn",
      );
    } else {
      pill(container, "代理库", "已关闭", "pill-off");
    }
    pill(container, "注入头", guard.header_name || "—", "");
    var lengthText = guard.target_state_length ? String(guard.target_state_length) : "不限";
    if (guard.model_length_overrides > 0) lengthText += "（" + guard.model_length_overrides + " 个模型单独配置）";
    pill(container, "默认满血长度", lengthText, "");
  }

  function renderModelCard(model, poolSize, defaultLength) {
    var card = node("div", "model-card" + (model.pending ? " model-card-pending" : ""));
    var head = node("div", "model-card-head");
    head.appendChild(node("span", "model-name", model.model));
    var size = model.size || poolSize || 0;
    head.appendChild(node("span", "model-count", (model.valid || 0) + " / " + size));
    card.appendChild(head);

    var meter = node("div", "meter");
    var fill = node("span", (model.valid || 0) > 0 ? "" : "meter-empty");
    var ratio = size > 0 ? Math.min(100, Math.round(((model.valid || 0) / size) * 100)) : 0;
    fill.style.width = ratio + "%";
    meter.appendChild(fill);
    card.appendChild(meter);

    // pending：模型只在当前草稿里、还没写进生效配置，运行时自然没有它的票池数据。
    // 只摆一张占位卡说明「保存后才建池」，不去渲染那些它根本没有的实况字段。
    if (model.pending) {
      card.appendChild(node("div", "model-meta", "待保存生效：这个模型还没写进已生效的配置，保存后插件才会为它建池撞票。"));
      return card;
    }

    if ((model.valid || 0) > 0) {
      card.appendChild(
        node(
          "div",
          "model-meta",
          "最长 " + humanSeconds(model.freshest_seconds) + " · 最短 " + humanSeconds(model.soonest_seconds),
        ),
      );
    }

    var round = [];
    if (model.reason) round.push(REASON_LABELS[model.reason] || model.reason);
    if (model.last_refill_seconds >= 0) round.push(humanSeconds(model.last_refill_seconds) + "前跑过一轮");
    else round.push("还没跑过");
    if (model.last_added > 0) round.push("新增 " + model.last_added + " 张");
    card.appendChild(node("div", "model-meta", round.join(" · ")));

    // 只在这个池的口径与默认值不同时标出来，免得每张卡都重复一遍总览已经写过的数字。
    var length = model.target_state_length || 0;
    if (length !== (defaultLength || 0)) {
      card.appendChild(node("div", "model-meta", "满血长度 " + (length ? String(length) : "不限")));
    }

    if (model.grades && Object.keys(model.grades).length > 0) {
      var grades = Object.keys(model.grades)
        .sort()
        .map(function (grade) {
          return (GRADE_LABELS[grade] || grade) + " " + model.grades[grade];
        });
      card.appendChild(node("div", "model-meta", "上一轮：" + grades.join(" · ")));
    }
    card.appendChild(node("div", "model-meta", "取票命中 " + (model.hits || 0) + " · 落空 " + (model.misses || 0)));
    if (model.detail) card.appendChild(node("div", "model-detail", model.detail));
    return card;
  }

  // mergeStatusModels 把「运行时实况」与「当前草稿里的模型清单」并成一组卡片：
  // 先按清单顺序摆——运行时有数据的用数据，没有的做「待保存生效」占位——再补上运行时里有、
  // 但清单没列的（follow_observed_models 自动跟踪出来的）模型。这样管理员往清单里加一行，
  // 上方立刻多出一张占位卡；删一行则该模型仍以实况卡留着，它得等保存生效后才真正停止维护。
  function mergeStatusModels(account, poolSize, draftModels) {
    var runtime = Array.isArray(account.models) ? account.models : [];
    var byName = {};
    runtime.forEach(function (model) {
      if (model && typeof model.model === "string") byName[model.model] = model;
    });
    var ordered = [];
    var used = {};
    (Array.isArray(draftModels) ? draftModels : []).forEach(function (name) {
      if (used[name]) return;
      used[name] = true;
      ordered.push(byName[name] || { model: name, valid: 0, size: poolSize, pending: true });
    });
    runtime.forEach(function (model) {
      if (!model || used[model.model]) return;
      used[model.model] = true;
      ordered.push(model);
    });
    return ordered;
  }

  function renderAccountStatus(account, poolSize, draftModels, defaultLength) {
    var box = node("div", "status-account");
    var head = node("div", "status-account-head");
    head.appendChild(node("span", "status-name", "账号 " + account.account_id));
    if (account.note) head.appendChild(node("span", "status-note", account.note));
    if (account.has_credential) {
      head.appendChild(node("span", "tag tag-on", "凭据 " + humanSeconds(account.credential_seconds) + "前采集"));
    } else {
      head.appendChild(node("span", "tag tag-warn", "等待第一个真实请求以获取凭据"));
    }
    box.appendChild(head);

    var models = mergeStatusModels(account, poolSize, draftModels);
    if (models.length === 0) {
      box.appendChild(node("p", "empty", "还没有任何模型的票池。"));
      return box;
    }
    var grid = node("div", "model-grid");
    models.forEach(function (model) {
      grid.appendChild(renderModelCard(model, poolSize, defaultLength));
    });
    box.appendChild(grid);
    return box;
  }

  function renderProxyStatus(proxies) {
    var box = el("status-proxies");
    box.textContent = "";
    var entries = Array.isArray(proxies.entries) ? proxies.entries : [];
    // 顺手把使用情况记下来，下面代理表的「状态」列直接读它。entry.addr 已是
    // scheme://打码地址，直接拿它当键。
    proxyUsage = {};
    entries.forEach(function (entry) {
      proxyUsage[entry.addr] = {
        success: entry.success || 0,
        fail: entry.fail || 0,
        last_ok: entry.last_ok,
      };
    });
    if (!proxies.enabled || entries.length === 0) return;

    box.appendChild(node("div", "status-subhead", "代理"));
    var list = node("div", "proxy-status-list");
    entries.forEach(function (entry) {
      var item = node("div", "proxy-status");
      if (entry.name) item.appendChild(node("span", "proxy-status-name", entry.name));
      // addr 自带协议前缀，不再单独摆一份 scheme。
      item.appendChild(node("span", "proxy-status-state", entry.addr));
      var used = (entry.success || 0) + (entry.fail || 0);
      if (used === 0) {
        item.appendChild(node("span", "tag tag-off", "未使用"));
      } else {
        item.appendChild(
          node("span", "tag " + (entry.success > 0 ? "tag-on" : "tag-warn"), usageText(entry)),
        );
      }
      list.appendChild(item);
    });
    box.appendChild(list);
  }

  // renderStatusAccounts 渲染上方「账号 × 模型」的算力票卡片组。拆成独立函数，是为了在
  // 模型清单被编辑时能就地重绘（见 scheduleAccountsRerender）：那时运行时数据还没变
  // （改动尚未保存生效），所以用最近一次看板数据（lastPayload）叠加当前草稿的模型清单。
  function renderStatusAccounts(payload) {
    var box = el("status-accounts");
    box.textContent = "";
    var accounts = payload && payload.tickets && Array.isArray(payload.tickets.accounts)
      ? payload.tickets.accounts
      : [];
    var poolSize = (payload && payload.guard && payload.guard.pool_size)
      || (draft && draft.overload_guard.ticket_pool.pool_size)
      || 0;
    var draftModels = draft ? draft.overload_guard.ticket_pool.models : [];
    // defaultLength 是总览药丸上那个默认口径，卡片只在自己的口径与它不同时才额外标出来。
    var defaultLength = (payload && payload.guard && payload.guard.target_state_length) || 0;
    accounts.forEach(function (account) {
      box.appendChild(renderAccountStatus(account, poolSize, draftModels, defaultLength));
    });
    el("status-empty").hidden = accounts.length > 0;
    el("status-empty").textContent = payload && payload.guard && !payload.guard.enabled
      ? "过载防护总开关已关闭：插件只做透传，不注入任何头。"
      : "还没有账号被接管，不会有任何撞票流量。";
  }

  function renderDashboard(view) {
    var payload = view.payload;

    if (!payload) {
      // 拿不到结构化数据（宿主回报插件未运行、老宿主截断了消息，或插件还没就绪）就退回纯文本。
      // 文本只在空态区展示一次，不再重复塞进下面的错误框。
      lastPayload = null;
      el("status-accounts").textContent = "";
      el("status-pills").textContent = "";
      el("status-proxies").textContent = "";
      proxyUsage = {};
      renderProxies();
      el("status-empty").textContent = emptyStateText(view);
      el("status-empty").hidden = false;
      setStatusError(noteLines(""));
      reportHeight();
      return;
    }

    lastPayload = payload;
    renderPills(payload);
    renderProxyStatus(payload.proxies || {});
    renderProxies();
    renderStatusAccounts(payload);

    var notes = [];
    // synced 只有 config.test 才判断得出（plugin.status 走 Health，宿主不带配置），
    // 读不到就不提示，避免在轮询里反复闪一条无法核实的警告。
    if (payload.synced === false) notes.push("这份配置还没有被应用（请先保存），下面展示的是当前生效配置的状态。");
    if (!view.success && view.text) notes.push(view.text);
    setStatusError(noteLines(notes.join("\n")));
    reportHeight();
  }

  // scheduleAccountsRerender 在模型清单变动后就地重绘卡片组（防抖，免得逐字符敲键时闪烁）。
  // syncDraft 已把新清单写进草稿，这里据此叠加 lastPayload 重绘；没有结构化票况时直接跳过，
  // 免得把「无数据」提示覆盖成空卡片组。
  function scheduleAccountsRerender() {
    if (!lastPayload) return;
    if (modelsRerenderTimer) window.clearTimeout(modelsRerenderTimer);
    modelsRerenderTimer = window.setTimeout(function () {
      modelsRerenderTimer = 0;
      renderStatusAccounts(lastPayload);
      reportHeight();
    }, 250);
  }

  // noteLines 把通道降级提示固定挂在状态区最前面，后面才是本次读取的问题。
  function noteLines(text) {
    var parts = [];
    if (statusNote) parts.push(statusNote);
    if (text) parts.push(text);
    return parts.join("\n");
  }

  // emptyStateText 是没有结构化票况时空态区要说的话：原样转述宿主/插件的消息
  // （例如「插件未运行」），只读通道还在轮询时顺带说明会自动恢复。
  function emptyStateText(view) {
    var text = view.text || "插件没有返回状态数据。";
    if (statusChannel === "status") {
      return "宿主回报：" + text + "（插件启用并就绪后，这里会自动刷新）";
    }
    return text;
  }

  function setStatusError(text) {
    var box = el("status-error");
    box.textContent = text;
    box.className = "diagnostic" + (text ? " bad" : "");
    box.hidden = text === "";
  }

  // showStatusFailure 把一次读取失败同时写进空态区与错误框：空态区不能一直停在
  // 「正在读取实时票况……」，错误框则是给读屏与眼睛都能立刻注意到的提示。
  function showStatusFailure(text) {
    if (!lastPayload) {
      el("status-empty").textContent = text;
      el("status-empty").hidden = false;
    }
    setStatusError(noteLines(text));
    reportHeight();
  }

  // openTest 在打开配置页时主动跑一次 config.test（需求：「点击插件弹窗时应自动调用一次测试」）。
  // 宿主对它做二次验证门控（可能弹出二次验证）、只在结果失败时才弹提示；但它是唯一在所有宿主
  // 版本上都带结构化票况的通道，所以两条通道下打开页面都先跑它一次，把上方的卡片组填出来。
  // 之后的自动刷新照旧走安静的 plugin.status（若可用）。
  function openTest() {
    if (statusPending) return Promise.resolve();
    statusPending = true;
    return bridge
      .testConfig()
      .then(viewFromTest)
      .then(renderDashboard)
      .catch(function (error) {
        showStatusFailure("打开时测试失败：" + error.message);
      })
      .then(function () {
        statusPending = false;
      });
  }

  // probeStatusChannel 只判定轮询通道，不渲染也不重复测试：openTest 会用 config.test
  // 铺好一屏，这里只问「宿主认不认识 plugin.status」。
  //   - 宿主回了（不管带不带票况，哪怕回的是「插件未运行」）或以非超时错误拒绝 → status，
  //     可静默轮询；插件未运行时空态区转述宿主消息，启用后下一次轮询自然恢复；
  //   - 宿主 < 0.2.7 不认识动词、直接丢掉消息（超时）→ test，只手动刷新。
  // 探明之前「刷新状态」一直禁用，两个分支都在这里把它放开。
  function probeStatusChannel() {
    return bridge
      .pluginStatus(STATUS_PROBE_TIMEOUT_MS)
      .then(function () {
        statusChannel = "status";
      })
      .catch(function (error) {
        if (error && error.timeout) {
          statusChannel = "test";
          statusNote = STATUS_NOTE_OLD_HOST;
        } else {
          statusChannel = "status";
        }
      })
      .then(function () {
        el("status-refresh").disabled = false;
      });
  }

  // readStatus 按已探明的通道读运行时状态：status = plugin.status（只读、免二次验证、
  // 不弹提示，可轮询）；test = 手动 config.test。通道在打开配置页时由 probeStatusChannel 定好，
  // 没探明之前谁也不许碰 plugin.status。
  function readStatus() {
    if (statusChannel === "test") {
      return bridge.testConfig().then(viewFromTest);
    }
    if (statusChannel === "status") {
      return bridge.pluginStatus(0).then(viewFromStatus);
    }
    return Promise.reject(new Error("状态通道尚未探明"));
  }

  // refreshStatus 是整页唯一读取运行时状态的地方：自动轮询与「刷新状态」按钮
  // 都走它，同一时刻只允许一次在途请求；通道没探明时直接不动。
  function refreshStatus() {
    if (statusPending || statusChannel === "unknown") return Promise.resolve();
    statusPending = true;
    return readStatus()
      .then(function (view) {
        renderDashboard(view);
      })
      .catch(function (error) {
        showStatusFailure("读取状态失败：" + error.message);
      })
      .then(function () {
        statusPending = false;
      });
  }

  function startStatusPolling() {
    if (statusTimer || statusChannel !== "status") return;
    statusTimer = window.setInterval(function () {
      // 页面被切到后台就不必读了：状态是即时快照，回到前台再拉一次就是最新的。
      if (document.hidden) return;
      refreshStatus();
    }, STATUS_POLL_MS);
  }

  function stopStatusPolling() {
    if (!statusTimer) return;
    window.clearInterval(statusTimer);
    statusTimer = 0;
  }

  /* ---------- 与宿主交互 ---------- */

  function applyLoaded(config) {
    draft = normalizeConfig(config);
    savedJSON = JSON.stringify(draft);
    renderForm();
    el("body").hidden = false;
    el("actions").hidden = false;
  }

  function load() {
    setBusy(true);
    return bridge
      .loadConfig()
      .then(function (config) {
        applyLoaded(config);
        showDiagnostic("", "");
      })
      .catch(function (error) {
        fatal("读取配置失败：" + error.message);
      })
      .then(function () {
        setBusy(false);
      });
  }

  function save() {
    syncDraft();
    var errors = validateDraft(draft);
    if (errors.length > 0) {
      showDiagnostic("bad", "配置有问题，未保存：\n" + errors.join("\n"));
      bridge.notify("error", "配置校验未通过，共 " + errors.length + " 处");
      return Promise.resolve(false);
    }
    setBusy(true);
    // 宿主保存成功会自己弹一次提示，这里只在失败时再报。
    return bridge
      .saveConfig(draft)
      .then(function (config) {
        applyLoaded(config);
        if (statusChannel === "status") {
          showDiagnostic("ok", "已保存。插件已按新配置重建票池与代理库；已经撞到的票和已采到的凭据不会丢。");
          // 配置刚换过，看板上的票池与代理统计都变了，立刻经只读通道读一次，别等下一个轮询周期。
          refreshStatus();
        } else {
          showDiagnostic("ok", "已保存。插件已按新配置重建票池与代理库；已经撞到的票和已采到的凭据不会丢。"
            + "看板要点「刷新状态」才会更新（会走 config.test）。");
          // test 通道不自动跑 config.test（宿主可能要求二次验证），只用上次的看板数据叠加
          // 刚保存的模型清单重绘卡片组，让「待保存生效」占位跟着草稿走。
          if (lastPayload) renderStatusAccounts(lastPayload);
        }
        return true;
      })
      .catch(function (error) {
        showDiagnostic("bad", "保存失败：" + error.message);
        bridge.notify("error", "保存失败：" + error.message);
        return false;
      })
      .then(function (ok) {
        setBusy(false);
        return ok;
      });
  }

  /* ---------- 高度上报 ---------- */

  function reportHeight() {
    if (heightTimer) return;
    heightTimer = window.setTimeout(function () {
      heightTimer = 0;
      if (anyModalOpen()) return; // 弹窗打开时已经申请了最大高度
      var height = Math.ceil(el("page").getBoundingClientRect().height) + 24;
      if (Math.abs(height - lastHeight) < 8) return;
      lastHeight = height;
      bridge.resize(height);
    }, 80);
  }

  /* ---------- 初始化 ---------- */

  function bindFormEvents() {
    var ids = ["tp-models", "tp-model-lengths", "tp-effort", "guard-enabled"];
    TICKET_NUMBERS.concat(TICKET_TEXTS, TICKET_FLAGS, PROXY_FLAGS).forEach(function (pair) {
      ids.push(pair[0]);
    });
    ids.forEach(function (id) {
      var input = el(id);
      input.addEventListener("input", syncDraft);
      input.addEventListener("change", syncDraft);
    });
    // 代理池总开关会改变代理列表里每行的状态标签，单独再挂一次重绘；
    // 过载防护总开关同理会改账号表的状态标签（「总开关已关闭」）。
    el("pp-enabled").addEventListener("change", renderProxies);
    el("guard-enabled").addEventListener("change", renderAccounts);
    // 模型清单一变，上方的账号模型算力票卡片组要跟着更新。syncDraft 已在前面注册、会先跑，
    // 把新清单写进草稿，这里据此就地重绘（防抖）。
    el("tp-models").addEventListener("input", scheduleAccountsRerender);
    el("tp-models").addEventListener("change", scheduleAccountsRerender);
  }

  function init() {
    if (!window.Sub2ApiBridge) {
      fatal("Bridge 脚本未加载");
      return;
    }
    draft = normalizeConfig({});

    bindFormEvents();
    el("btn-save").addEventListener("click", function () {
      if (busy) return;
      save();
    });
    el("btn-reload").addEventListener("click", function () {
      if (busy) return;
      load().then(function () {
        showDiagnostic("", "已重新载入已保存的配置。");
        // 草稿里的模型清单被换回已保存那份，上方卡片组里的「待保存生效」占位也要跟着退掉。
        if (lastPayload) renderStatusAccounts(lastPayload);
      });
    });

    el("account-add").addEventListener("click", function () {
      openAccountModal(-1);
    });
    el("account-modal-confirm").addEventListener("click", confirmAccountModal);
    el("proxy-add").addEventListener("click", function () {
      openProxyModal(-1);
    });
    el("proxy-modal-confirm").addEventListener("click", confirmProxyModal);
    el("proxy-addr").addEventListener("change", syncProxyAddr);
    el("proxy-addr").addEventListener("paste", function () {
      window.setTimeout(syncProxyAddr, 0);
    });

    Array.prototype.forEach.call(document.querySelectorAll("[data-close]"), function (button) {
      button.addEventListener("click", function () {
        closeModal(button.getAttribute("data-close"));
      });
    });
    ["account-modal", "proxy-modal"].forEach(function (id) {
      el(id).addEventListener("click", function (event) {
        if (event.target === el(id)) closeModal(id);
      });
    });
    document.addEventListener("keydown", function (event) {
      if (event.key !== "Escape") return;
      if (!el("proxy-modal").hidden) closeModal("proxy-modal");
      else if (!el("account-modal").hidden) closeModal("account-modal");
    });

    el("status-refresh").addEventListener("click", function () {
      refreshStatus();
    });
    // 回到前台立刻补一次，免得切回来先看到一屏过期数据。
    document.addEventListener("visibilitychange", function () {
      if (!document.hidden && statusChannel === "status") refreshStatus();
    });

    if (typeof window.ResizeObserver === "function") {
      new window.ResizeObserver(reportHeight).observe(el("page"));
    } else {
      window.addEventListener("resize", reportHeight);
    }
    window.addEventListener("pagehide", function () {
      stopStatusPolling();
      bridge.dispose();
    });

    bridge.ready();
    load()
      .then(function () {
        // 先探明轮询通道（顺带放开「刷新状态」按钮）并可能写下降级提示；这一步不渲染、也不测试。
        return probeStatusChannel();
      })
      .then(function () {
        // 打开配置页主动跑一次 config.test，立刻把上方的卡片组填出来（渲染时会带上降级提示）。
        return openTest();
      })
      .then(function () {
        // 只有只读通道（status）才自动轮询；test 通道只手动刷新。
        startStatusPolling();
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
