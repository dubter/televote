package vote

import (
	_ "embed"

	"github.com/redis/rueidis"
)

//go:embed vote.lua
var voteScriptSource string

var voteScript = rueidis.NewLuaScript(voteScriptSource)

const ballotsField = "b"
