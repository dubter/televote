package domain

import "github.com/google/uuid"

type Admin struct {
	ID           uuid.UUID
	Login        string
	PasswordHash string
	Role         string
}
