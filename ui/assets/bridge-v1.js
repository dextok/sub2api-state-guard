/**
 * UI Bridge v1 客户端。
 *
 * 宿主把配置页放进 sandbox="allow-scripts" 的 iframe，不给 allow-same-origin，
 * 也不给管理员 Token：这里能用的只有 postMessage 上的那几个消息
 * （config.load / config.save / config.test / plugin.status / ui.resize / ui.notify）。
 *
 * 安全要求（backend/pkg/pluginapi/docs/ui-bridge.md）：
 *   - 只接受 event.source === parent 的消息；
 *   - 校验 source === "sub2api-plugin-host" 与 bridge_token；
 *   - 只处理自己正在等待的 request_id；
 *   - 每个请求都要有超时，页面卸载时清理。
 */
(function (global) {
  "use strict";

  var UI_SOURCE = "sub2api-plugin-ui";
  var HOST_SOURCE = "sub2api-plugin-host";
  var REQUEST_TIMEOUT_MS = 30000;

  function readBridgeToken() {
    // Bridge Token 只存在于 URL fragment，不会被发送到服务器。
    var fragment = String(global.location.hash || "").replace(/^#/, "");
    var parts = fragment.split("&");
    for (var index = 0; index < parts.length; index += 1) {
      var pair = parts[index].split("=");
      if (pair[0] === "bridge_token" && pair.length > 1) {
        return decodeURIComponent(pair.slice(1).join("="));
      }
    }
    return "";
  }

  function Bridge() {
    this.token = readBridgeToken();
    this.pending = Object.create(null);
    this.sequence = 0;
    this.disposed = false;
    this.onMessage = this.handleMessage.bind(this);
    global.addEventListener("message", this.onMessage);
  }

  Bridge.prototype.available = function () {
    return !this.disposed && this.token !== "" && global.parent && global.parent !== global;
  };

  Bridge.prototype.nextRequestID = function () {
    this.sequence += 1;
    var random = Math.random().toString(36).slice(2, 10);
    return "ui-" + this.sequence + "-" + random;
  };

  Bridge.prototype.handleMessage = function (event) {
    if (this.disposed || event.source !== global.parent) return;
    var data = event.data;
    if (!data || typeof data !== "object") return;
    if (data.source !== HOST_SOURCE || data.bridge_token !== this.token) return;

    var requestID = typeof data.request_id === "string" ? data.request_id : "";
    var entry = requestID ? this.pending[requestID] : null;
    if (!entry) return;
    delete this.pending[requestID];
    global.clearTimeout(entry.timer);

    if (data.ok || entry.settleAny) {
      entry.resolve(data);
      return;
    }
    entry.reject(new Error(typeof data.error === "string" && data.error ? data.error : "宿主拒绝了该操作"));
  };

  Bridge.prototype.post = function (payload) {
    if (!this.available()) return false;
    var envelope = { source: UI_SOURCE, bridge_token: this.token };
    for (var key in payload) {
      if (Object.prototype.hasOwnProperty.call(payload, key)) envelope[key] = payload[key];
    }
    // sandbox iframe 是不透明来源，没有固定的 targetOrigin 可用；
    // 安全性由 bridge_token 与 request_id 配对保证。
    global.parent.postMessage(envelope, "*");
    return true;
  };

  Bridge.prototype.request = function (type, payload, settleAny, timeoutMs) {
    var self = this;
    return new Promise(function (resolve, reject) {
      if (!self.available()) {
        reject(new Error("未检测到宿主 Bridge，请从插件管理页打开本配置页"));
        return;
      }
      var wait = Number(timeoutMs) > 0 ? Number(timeoutMs) : REQUEST_TIMEOUT_MS;
      var requestID = self.nextRequestID();
      var timer = global.setTimeout(function () {
        delete self.pending[requestID];
        var error = new Error("宿主在 " + Math.round(wait / 1000) + " 秒内没有响应 " + type
          + "（如果弹出了二次验证，请重新操作）");
        // 老宿主遇到不认识的动词会直接丢掉消息，表现就是超时；
        // 调用方据此判断「这条通道不可用」并降级，而不是当成一次普通失败。
        error.timeout = true;
        reject(error);
      }, wait);
      self.pending[requestID] = {
        resolve: resolve,
        reject: reject,
        timer: timer,
        settleAny: settleAny === true,
      };

      var message = { type: type, request_id: requestID };
      for (var key in payload) {
        if (Object.prototype.hasOwnProperty.call(payload, key)) message[key] = payload[key];
      }
      if (!self.post(message)) {
        delete self.pending[requestID];
        global.clearTimeout(timer);
        reject(new Error("无法向宿主发送消息"));
      }
    });
  };

  Bridge.prototype.ready = function () {
    this.post({ type: "sub2api.plugin.ready" });
  };

  Bridge.prototype.loadConfig = function () {
    return this.request("config.load", {}).then(function (data) {
      return data.config && typeof data.config === "object" && !Array.isArray(data.config) ? data.config : {};
    });
  };

  Bridge.prototype.saveConfig = function (config) {
    return this.request("config.save", { config: config }).then(function (data) {
      return data.config && typeof data.config === "object" && !Array.isArray(data.config) ? data.config : config;
    });
  };

  // 宿主在测试失败时回的是 ok=false + result（而不是 error 字段），
  // 所以这里放行失败结果，把 success/message 交给页面展示。
  Bridge.prototype.testConfig = function () {
    return this.request("config.test", {}, true).then(function (data) {
      if (data.result && typeof data.result === "object") return data.result;
      if (data.ok) return { success: true, message: "" };
      throw new Error(typeof data.error === "string" && data.error ? data.error : "测试失败");
    });
  };

  // pluginStatus 读取插件的被动运行时状态（宿主把这个动词映射到插件 Health）。
  //
  // 它是 UI Bridge v1 里唯一适合轮询的通道：只读、免二次验证、宿主不会弹提示。
  // sub2api < 0.2.7 不认识 plugin.status，会把消息直接丢掉——调用方应传一个较短的
  // timeoutMs 去探测，拿到 error.timeout 就降级成手动 config.test。
  Bridge.prototype.pluginStatus = function (timeoutMs) {
    return this.request("plugin.status", {}, false, timeoutMs).then(function (data) {
      if (data.result && typeof data.result === "object") return data.result;
      throw new Error("宿主没有返回状态数据");
    });
  };

  Bridge.prototype.resize = function (height) {
    var value = Math.round(Number(height));
    if (!isFinite(value) || value <= 0) return;
    this.post({ type: "ui.resize", height: value });
  };

  Bridge.prototype.notify = function (level, message) {
    var text = String(message || "").slice(0, 500);
    if (!text) return;
    this.post({ type: "ui.notify", level: level, message: text });
  };

  Bridge.prototype.dispose = function () {
    this.disposed = true;
    global.removeEventListener("message", this.onMessage);
    for (var requestID in this.pending) {
      if (!Object.prototype.hasOwnProperty.call(this.pending, requestID)) continue;
      global.clearTimeout(this.pending[requestID].timer);
      this.pending[requestID].reject(new Error("配置页已关闭"));
    }
    this.pending = Object.create(null);
  };

  global.Sub2ApiBridge = { create: function () { return new Bridge(); } };
})(window);
