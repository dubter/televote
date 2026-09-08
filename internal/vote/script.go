package vote

import (
	_ "embed"

	"github.com/redis/rueidis"
)

//go:embed vote.lua
var voteScriptSource string

// voteScript загружается один раз на процесс. rueidis сам вызывает EVALSHA и
// откатывается на EVAL при NOSCRIPT — это важно после failover, когда на новой
// ноде скрипта ещё нет.
var voteScript = rueidis.NewLuaScript(voteScriptSource)

// Поле хэша счётчиков, хранящее число бюллетеней. Отдельно от голосов, потому
// что при множественном выборе один бюллетень даёт несколько голосов, и
// проценты обязаны считаться от бюллетеней.
const ballotsField = "b"
