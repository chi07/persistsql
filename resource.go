// Package persistsql provides generic persistence functions on top of PostgresSQL and github.com/go-pg/pg.
package persistsql

import (
	"context"
	"fmt"

	"github.com/go-pg/pg/v10"
	"github.com/go-pg/pg/v10/orm"

	"github.com/chi07/resource"
)

type QueryHook func(query *orm.Query)

// RawQuery is a raw PostgresSQL query.
type RawQuery struct {
	// Query text
	Q string
	// True to ignore errors
	ErrOk bool
}

// SQL represents a persistence layer for resources based on SQL.
type SQL struct {
	db         *pg.DB
	notifyStmt *pg.Stmt
}

// New creates an SQL persistence layer backed by db.
func New(db *pg.DB) (*SQL, error) {
	notifyStmt, err := db.Prepare("SELECT pg_notify('events', $1)")
	if err != nil {
		return nil, fmt.Errorf("db.Prepare(): %w", err)
	}

	return &SQL{
		db:         db,
		notifyStmt: notifyStmt,
	}, nil
}

// CreateTables ensures all tables needed to store the models exist, it then runs the raw queries, if non-nil.
// All happens in a single transaction.
func (p *SQL) CreateTables(ctx context.Context, models []interface{}, rawQueries []RawQuery) error {
	return p.db.WithContext(ctx).RunInTransaction(ctx, func(tx *pg.Tx) error {
		for _, model := range models {
			cto := orm.CreateTableOptions{
				IfNotExists:   true,
				FKConstraints: true,
			}

			if err := tx.Model(model).CreateTable(&cto); err != nil {
				return err
			}
		}

		if rawQueries != nil {
			for _, curr := range rawQueries {
				if _, err := tx.ExecOne(curr.Q); err != nil && !curr.ErrOk {
					return err
				}
			}
		}

		return nil
	})
}

// CreateResource inserts a single resource into the table representing the collection.
func (p *SQL) CreateResource(ctx context.Context, resource resource.Resource) (resource.Resource, error) {
	if err := p.db.WithContext(ctx).RunInTransaction(ctx, func(tx *pg.Tx) error {
		if _, err := tx.Model(resource).Insert(); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return resource, nil
}

// ShowDeleted modifies an orm.Query to not filter out soft deleted rows if showDeleted is true.
// If showDeleted is false, ShowDeleted does not modify the query.
func ShowDeleted(query *orm.Query, showDeleted bool) {
	if showDeleted {
		query.AllWithDeleted()
	}
}

// GetResource retrieves a single resource from a collection.
// The query is built without a WHERE clause and SELECT all fields of the resource.
// showDeleted controls whether soft-deleted resources are allowed to be returned.
// QueryHook is called before executing the query, to be used for adding a WHERE clause or for other adjustments.
func (p *SQL) GetResource(ctx context.Context, resource resource.Resource, showDeleted bool, queryHook QueryHook) (resource.Resource, error) {
	query := p.db.ModelContext(ctx, resource)
	ShowDeleted(query, showDeleted)
	queryHook(query)

	if err := query.Select(); err != nil {
		if err == pg.ErrNoRows {
			return nil, nil
		}

		return nil, err
	}

	return resource, nil
}

// UpdateResource updates a resource in a collection.
// The query is built without a WHERE clause and updates the fields of the model listed in the fields slice and updated_at.
// QueryHook is called before executing the query, to be used for adding a WHERE clause or for other adjustments.
func (p *SQL) UpdateResource(ctx context.Context, resource resource.Resource, fields []string, queryHook QueryHook) (resource.Resource, error) {
	if err := p.db.WithContext(ctx).RunInTransaction(ctx, func(tx *pg.Tx) error {
		query := tx.Model(resource).Returning("*").Column("updated_at")
		for _, col := range fields {
			query.Column(col)
		}

		queryHook(query)

		if _, err := query.Update(); err != nil {
			return err
		}

		return nil
	}); err != nil {
		if err == pg.ErrNoRows {
			return nil, nil
		}

		return nil, err
	}

	return resource, nil
}

// DeleteResource deletes a resource from a collection.
// The query is built with a WHERE clause to match the primary key of the model. If QueryHook is non-nil, it is called before executing the query.
func (p *SQL) DeleteResource(ctx context.Context, resource resource.Resource, queryHook QueryHook) (resource.Resource, error) {
	if err := p.db.WithContext(ctx).RunInTransaction(ctx, func(tx *pg.Tx) error {
		query := tx.Model(resource).WherePK().Returning("*")
		if queryHook != nil {
			queryHook(query)
		}

		if _, err := query.Delete(); err != nil {
			return err
		}

		return nil
	}); err != nil {
		if err == pg.ErrNoRows {
			return nil, nil
		}

		return nil, err
	}

	return resource, nil
}

// UndeleteResource undeletes a soft-deleted resource from a collection.
// The query is built with a WHERE clause to match the primary key of the model. If QueryHook is non-nil, it is called before executing the query.
func (p *SQL) UndeleteResource(ctx context.Context, resource resource.Resource, queryHook QueryHook) (resource.Resource, error) {
	if err := p.db.WithContext(ctx).RunInTransaction(ctx, func(tx *pg.Tx) error {
		query := tx.Model(resource).WherePK().Deleted().Column("deleted_at").Returning("*")
		if queryHook != nil {
			queryHook(query)
		}

		if _, err := query.Update(); err != nil {
			return err
		}

		return nil
	}); err != nil {
		if err == pg.ErrNoRows {
			return nil, nil
		}

		return nil, err
	}

	return resource, nil
}
