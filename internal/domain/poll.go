// Package domain описывает опрос как предметную сущность: тип и опции,
// правила выбора, конечный автомат статусов, агрегат результатов и расчёт
// числа шардов.
//
// Пакет не импортирует ничего из проекта и почти ничего извне — только stdlib
// и github.com/google/uuid. Это проверяет depguard (правило domain-is-pure в
// .golangci.yml). Смысл ограничения не в чистоте: как только домен узнаёт про
// HTTP или Redis, правила голосования начинают зависеть от транспорта, и их
// уже нельзя проверить одним быстрым юнит-тестом — а именно этими тестами
// закрыты инварианты, которые ломаются молча.
package domain

import (
	"time"

	"github.com/google/uuid"
)

// PollType — тип опроса. Шкала, рейтинг и «да/нет» укладываются в single:
// это выбор одного варианта над упорядоченными опциями (requirements.md §1).
type PollType string

// Типы опроса. Список закрытый: свободный текст не имеет агрегата и вне скоупа.
const (
	// PollTypeSingle — ровно один вариант независимо от min/max.
	PollTypeSingle PollType = "single"
	// PollTypeMultiple — от MinChoices до MaxChoices вариантов.
	PollTypeMultiple PollType = "multiple"
)

// Valid сообщает, известен ли домену такой тип. Тип приходит из базы строкой,
// поэтому проверка нужна на границе, а не в вере в схему.
func (t PollType) Valid() bool {
	switch t {
	case PollTypeSingle, PollTypeMultiple:
		return true
	default:
		return false
	}
}

// Status — состояние опроса в жизненном цикле. Хранится явно и никогда не
// выводится из дат: вычисляемый статус зависит от часов машины, и опрос,
// открытый на инстансе с уехавшим временем, принимал бы голоса вне эфира.
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

// MaxOptions — предел числа опций: индекс опции занимает uint8 и служит полем
// хэша счётчиков в Redis, поэтому адресуемых опций не больше 255.
const MaxOptions = 255

// Option — вариант ответа. Адресуется целым индексом, а не UUID: индекс
// становится полем хэша счётчиков в Redis, а payload голоса — {"choices":[2]}.
type Option struct {
	// Idx — позиция опции, плотная нумерация с нуля.
	Idx uint8
	// Text — текст варианта, показывается зрителю.
	Text string
}

// Poll — опрос целиком, как он лежит в Postgres. На горячем пути не
// используется: там работают срезы ChoiceRules и Window, собранные из
// кэшированного конфига.
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
	// смена значения в эфире перевела бы проголосовавших на другие шарды и
	// открыла повторное голосование.
	ShardCount uint16
	// ResultsVisibleDuringVoting по умолчанию false: промежуточный счёт влияет
	// на непроголосовавших и портит опрос.
	ResultsVisibleDuringVoting bool
	// Version — optimistic locking, чтобы два админа не затёрли правки друг друга.
	Version uint32
}

// OptionCount возвращает число опций, насыщаясь на MaxOptions. Насыщение, а не
// приведение: uint8(256) дало бы 0 и тихо отвергло все голоса опроса.
func (p *Poll) OptionCount() uint8 {
	if len(p.Options) > MaxOptions {
		return MaxOptions
	}
	return uint8(len(p.Options))
}

// Число шардов дедупа и счётчиков.
const (
	// ShardsPerMaster — сколько шардов приходится на один мастер Redis.
	// Перекос нагрузки между мастерами равен 1/√(шардов на мастера):
	// 500 держат его в пределах ±4.5 % (design.md §4).
	ShardsPerMaster = 500

	// MaxShardCount — потолок, равный числу слотов Redis Cluster: больше
	// шардов уже не улучшает раскладку по слотам, а fan-in при агрегации
	// растёт линейно.
	MaxShardCount = 16384
)

// ShardCountFor считает shard_count для кластера из masters мастеров:
// ≈500 на мастера, но не больше MaxShardCount.
//
// Значение вычисляется один раз при создании опроса и записывается в его
// строку. Ноль не возвращается никогда: shard = hash % shard_count, и ноль
// здесь — паника деления на ноль на горячем пути.
func ShardCountFor(masters int) uint16 {
	if masters <= 1 {
		return ShardsPerMaster
	}
	// Сравнение до умножения: masters приходит из конфига и при абсурдном
	// значении переполнил бы int.
	if masters > MaxShardCount/ShardsPerMaster {
		return MaxShardCount
	}
	return uint16(masters * ShardsPerMaster)
}
