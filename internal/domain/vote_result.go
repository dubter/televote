package domain

type VoteResult uint8

const (
	VoteCounted        VoteResult = 1
	VoteAlreadyCounted VoteResult = 2
)

func (r VoteResult) Valid() bool { return r == VoteCounted || r == VoteAlreadyCounted }

func (r VoteResult) String() string {
	switch r {
	case VoteCounted:
		return "counted"
	case VoteAlreadyCounted:
		return "already_counted"
	default:
		return "invalid"
	}
}
