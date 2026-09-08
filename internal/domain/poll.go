// Package domain описывает опрос: типы, правила выбора, FSM статусов и агрегат.
//
// Не импортирует ничего из проекта — это проверяет depguard.
package domain

import (
	"time"

	"github.com/google/uuid"
)

// PollType — тип опроса. Шкала, рейтинг и «да/нет» — это single.
type PollType string

// Свободный текст не поддерживается: у него нет агрегата.
const (
	// PollTypeSingle — ровно один вариант независимо от min/max.
	PollTypeSingle PollType = "single"
	// PollTypeMultiple — от MinChoices до MaxChoices вариантов.
	PollTypeMultiple PollType = "multiple"
)

// Valid сообщает, известен ли тип: из базы он приходит строкой.
func (t PollType) Valid() bool {
	switch t {
	case PollTypeSingle, PollTypeMultiple:
		return true
	default:
		return false
	}
}

// Status хранится явно и не выводится из дат: иначе состояние опроса
// зависело бы от часов конкретной машины.
type Status string

// Статусы опроса. Переходы между ними задаёт CanTransitionTo.
const (
	// StatusDraft — создан, невидим публично.
	StatusDraft Status = "draft"
	// StatusScheduled — ждёт OpensAt; открывается планировщиком (ShouldOpenAt).
	StatusScheduled Status = "scheduled"
	// StatusOpen — принимает голоса внутри окна.
	StatusOpen Status = "open"
	// StatusClosed — голосование завершено, результат финальный.
	StatusClosed Status = "closed"
	// StatusArchived — терминальный статус, выхода нет.
	StatusArchived Status = "archived"
)

// Valid сообщает, известен ли домену такой статус.
func (s Status) Valid() bool {
	switch s {
	case StatusDraft, StatusScheduled, StatusOpen, StatusClosed, StatusArchived:
		return true
	default:
		return false
	}
}

// MaxOptions — предел: индекс опции занимает uint8.
const MaxOptions = 255

// Option адресуется целым индексом, а не UUID: индекс становится полем хэша
// счётчиков в Redis.
type Option struct {
	// Idx — позиция опции, плотная нумерация с нуля.
	Idx uint8
	// Text — текст варианта, показывается зрителю.
	Text string
}

// Poll — опрос целиком. На горячем пути не используется: там работают
// ChoiceRules и Window из кэшированного конфига.
type Poll struct {
	ID         uuid.UUID
	Slug       string
	Question   string
	Type       PollType
	Options    []Option
	MinChoices uint8
	MaxChoices uint8
	Status     Status
	OpensAt    time.Time
	ClosesAt   time.Time
	// ShardCount фиксируется в строке опроса, а не берётся из конфига сервиса:
	ShardCount                 uint16
	ResultsVisibleDuringVoting bool
	Salt                       []byte
	// ExpectedAudience и ExpectedConversion задают ожидаемую нагрузку эфира:
	ExpectedAudience   int64
	ExpectedConversion float64
	// Version — optimistic locking, чтобы два админа не затёрли правки друг друга.
	Version uint32
}

// OptionCount насыщается на MaxOptions: uint8(256) дало бы 0 и тихо отвергло
// все голоса опроса.
func (p *Poll) OptionCount() uint8 {
	if len(p.Options) > MaxOptions {
		return MaxOptions
	}
	return uint8(len(p.Options))
}

// Число шардов дедупа и счётчиков.
const (
	ShardsPerMaster = 500

	MaxShardCount = 16384
)

// ShardCountFor считает shard_count: ≈500 на мастера, не больше MaxShardCount.
// Ноль не возвращается никогда — это было бы деление на ноль на горячем пути.
func ShardCountFor(masters int) uint16 {
	if masters <= 1 {
		return ShardsPerMaster
	}
	// Сравнение до умножения: абсурдное значение переполнило бы int.
	if masters > MaxShardCount/ShardsPerMaster {
		return MaxShardCount
	}
	return uint16(masters * ShardsPerMaster)
}
