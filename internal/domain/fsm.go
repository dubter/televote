package domain

var transitions = map[Status]Status{
	StatusDraft:     StatusScheduled,
	StatusScheduled: StatusOpen,
	StatusOpen:      StatusClosed,
	StatusClosed:    StatusArchived,
}

func (s Status) CanTransitionTo(next Status) bool {
	allowed, ok := transitions[s]
	return ok && allowed != "" && allowed == next
}
