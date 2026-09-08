package domain

import "time"

type ChoiceRules struct {
	Type        PollType
	OptionCount uint8
	MinChoices  uint8
	MaxChoices  uint8
}

func (p *Poll) ChoiceRules() ChoiceRules {
	return ChoiceRules{
		Type:        p.Type,
		OptionCount: p.OptionCount(),
		MinChoices:  p.MinChoices,
		MaxChoices:  p.MaxChoices,
	}
}

func (r ChoiceRules) Validate(choices []uint8) error {
	if len(choices) == 0 {
		return ErrInvalidChoices
	}

	switch r.Type {
	case PollTypeSingle:
		if len(choices) != 1 {
			return ErrInvalidChoices
		}
	case PollTypeMultiple:
		n := len(choices)
		if n < int(r.MinChoices) {
			return ErrInvalidChoices
		}
		if r.MaxChoices > 0 && n > int(r.MaxChoices) {
			return ErrInvalidChoices
		}
	default:
		return ErrInvalidChoices
	}

	var seen [MaxOptions + 1]bool
	for _, idx := range choices {
		if idx >= r.OptionCount {
			return ErrInvalidChoices
		}
		if seen[idx] {
			return ErrInvalidChoices
		}
		seen[idx] = true
	}
	return nil
}

type Window struct {
	Status   Status
	OpensAt  time.Time
	ClosesAt time.Time
}

func (p *Poll) Window() Window {
	return Window{Status: p.Status, OpensAt: p.OpensAt, ClosesAt: p.ClosesAt}
}

func (p *Poll) IsOpenAt(t time.Time) bool {
	return p.Window().IsOpenAt(t)
}

func (w Window) IsOpenAt(t time.Time) bool {
	return w.Status == StatusOpen && w.Contains(t)
}

func (w Window) Contains(t time.Time) bool {
	if w.ClosesAt.IsZero() {
		return false
	}
	return !t.Before(w.OpensAt) && t.Before(w.ClosesAt)
}

func (p *Poll) ShouldOpenAt(t time.Time) bool {
	if p.Status != StatusScheduled || p.OpensAt.IsZero() {
		return false
	}
	return !t.Before(p.OpensAt)
}
