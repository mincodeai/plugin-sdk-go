package main

// leetcode-cn talks to leetcode.cn only after sign-in, so every recorded
// action is answered locally: status/logout without a session, payload and
// argument validation, and run/submit rejected before any network I/O. The
// "tasks-fallback" scenario pins what a host that does not negotiate tasks
// sees (task methods answer -32601 "method not found").
func init() {
	plugins["leetcode-cn"] = plugin{
		local: [2]string{"leetcode.logout", "{}"},
		extra: func() []scenario {
			const judge = `{"titleSlug":"two-sum","lang":"cpp","code":"class Solution {};"}`
			return []scenario{
				{"actions", []step{
					s(req(1, "plugin.initialize", initPlain)),
					s(invoke(2, "leetcode.status", "{}")),
					s(invoke(3, "leetcode.status", "{bad")),
					s(invoke(4, "leetcode.status", "")),
					s(invoke(5, "leetcode.logout", "{}")),
					s(invoke(6, "leetcode.detail", `{"titleSlug":"Bad_Slug"}`)),
					s(invoke(7, "leetcode.detail", "")),
					s(invoke(8, "leetcode.detail", `{"titleSlug":"two-sum","x":1}`)),
					s(invoke(9, "leetcode.detail", "[1]")),
					s(invoke(10, "leetcode.list", `{"keyword":"@@PAD81@@"}`)),
					s(invoke(11, "leetcode.list", `{"limit":"1"}`)),
					s(invoke(12, "leetcode.run", "{}")),
					s(invoke(13, "leetcode.run", `{"titleSlug":"two-sum","lang":"c++\";","code":"x"}`)),
					s(invoke(14, "leetcode.run", `{"titleSlug":"two-sum","lang":"cpp","code":"  "}`)),
					s(invoke(15, "leetcode.run", `{"titleSlug":"two-sum","lang":"cpp","code":"x","dataInput":"@@PAD16385@@"}`)),
					s(invoke(16, "leetcode.run", judge)),
					s(invoke(17, "leetcode.submit", judge)),
					s(invoke(18, "leetcode.submit", `{"titleSlug":"two-sum","lang":"cpp","code":"x","extra":true}`)),
					s(invoke(19, "leetcode.login", `{"session":"a b"}`)),
					s(invoke(20, "leetcode.login", `{"session":"abc","csrfToken":"x;y"}`)),
					s(invoke(21, "leetcode.login", `{"cookieHeader":"other=1"}`)),
					s(invoke(22, "leetcode.login", `{"cookieHeader":"LEETCODE_SESSION=one; LEETCODE_SESSION=two"}`)),
					s(invoke(23, "leetcode.login-browser", `{"unknown":1}`)),
					s(invoke(24, "leetcode.nope", "{}")),
					s(invoke(25, "", "{}")),
					s(req(26, "plugin.shutdown", "")),
					step{eof: true},
				}},
				{"tasks-fallback", []step{
					s(req(1, "plugin.initialize", initPlain)),
					s(req(2, "plugin.task.start", `{"taskId":"t1","action":"leetcode.submit","payload":`+quote(judge)+`}`)),
					s(req(3, "plugin.task.status", `{"taskId":"t1"}`)),
					s(req(4, "plugin.task.cancel", `{"taskId":"t1"}`)),
					s(invoke(5, "leetcode.submit", judge)),
					s(req(6, "plugin.shutdown", "")),
					step{eof: true},
				}},
			}
		},
	}
}

func quote(v string) string {
	b := []byte{'"'}
	for _, r := range v {
		if r == '"' || r == '\\' {
			b = append(b, '\\')
		}
		b = append(b, string(r)...)
	}
	return string(append(b, '"'))
}
