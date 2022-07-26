package model

import (
	"time"

	"github.com/google/uuid"
)

type Common struct {
	tableName struct{} `pg:",discard_unknown_columns"`

	ID         uuid.UUID `pg:",pk,type:uuid" filter:"-"`
	CreateTime time.Time `pg:",notnull"`
	UpdateTime time.Time `pg:",notnull"`
	DeleteTime time.Time `pg:",soft_delete" filter:"-"`
	Version    uint64    `pg:",notnull,default:1" filter:"-"`
}
