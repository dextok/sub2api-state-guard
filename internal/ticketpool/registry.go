package ticketpool

// Registry 是进程级的票池与凭据仓库，跨 ApplyConfig 存活。
//
// 配置一改就重建 Manager，但票还有几十分钟有效期、凭据只能等下一个真实请求才能重采，
// 跟着配置一起扔掉意味着每次保存配置都要经历一段"无票期"。
//
// 两张表在构造后不再被替换，各自内部加锁，所以这里不需要额外的互斥。
type Registry struct {
	credentials *CredentialStore
	tickets     *Store
}

// NewRegistry 创建仓库。整个插件进程只需要一个。
func NewRegistry() *Registry {
	return &Registry{
		credentials: NewCredentialStore(),
		tickets:     NewStore(),
	}
}

// Credentials 返回凭据表。
func (r *Registry) Credentials() *CredentialStore {
	return r.credentials
}

// Tickets 返回票池表。
func (r *Registry) Tickets() *Store {
	return r.tickets
}

// Retain 丢弃不再被接管的账号的凭据与票。
// 管理员关掉一个账号之后，它的 access token 不该继续留在插件内存里。
//
// 调用方必须先停掉旧的 Manager 再调用：否则旧循环里在途的探针会在这之后
// 把刚抹掉的票重新写回去。
func (r *Registry) Retain(active map[int64]struct{}) {
	r.credentials.Retain(active)
	r.tickets.Retain(active)
}
