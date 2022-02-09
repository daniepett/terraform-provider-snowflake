package snowflake

import (
	"fmt"
)

type allGrantType string
type allGrantTarget string

const (
	allSchemaType           allGrantType = "SCHEMA"
	allTableType            allGrantType = "TABLE"
	allViewType             allGrantType = "VIEW"
	allMaterializedViewType allGrantType = "MATERIALIZED VIEW"
	allStageType            allGrantType = "STAGE"
	allExternalTableType    allGrantType = "EXTERNAL TABLE"
	allFileFormatType       allGrantType = "FILE FORMAT"
	allFunctionType         allGrantType = "FUNCTION"
	allProcedureType        allGrantType = "PROCEDURE"
	allSequenceType         allGrantType = "SEQUENCE"
	allStreamType           allGrantType = "STREAM"
	allPipeType             allGrantType = "PIPE"
	allTaskType             allGrantType = "TASK"
)

const (
	allSchemaTarget   allGrantTarget = "SCHEMA"
	allDatabaseTarget allGrantTarget = "DATABASE"
)

// AllGrantBuilder abstracts the creation of AllGrantExecutables
type AllGrantBuilder struct {
	name           string
	qualifiedName  string
	allGrantType   allGrantType
	allGrantTarget allGrantTarget
}

func getAllNameAndQualifiedName(db, schema string) (string, string, allGrantTarget) {
	name := schema
	allTarget := allSchemaTarget
	qualifiedName := fmt.Sprintf(`"%v"."%v"`, db, schema)

	if schema == "" {
		name = db
		allTarget = allDatabaseTarget
		qualifiedName = fmt.Sprintf(`"%v"`, db)
	}

	return name, qualifiedName, allTarget
}

// Name returns the object name for this AllGrantBuilder
func (fgb *AllGrantBuilder) Name() string {
	return fgb.name
}

func (fgb *AllGrantBuilder) GrantType() string {
	return string(fgb.allGrantType)
}

// AllSchemaGrant returns a pointer to a AllGrantBuilder for a schema
func AllSchemaGrant(db string) GrantBuilder {
	return &AllGrantBuilder{
		name:           db,
		qualifiedName:  fmt.Sprintf(`"%v"`, db),
		allGrantType:   allSchemaType,
		allGrantTarget: allDatabaseTarget,
	}
}

// AllTableGrant returns a pointer to a AllGrantBuilder for a table
func AllTableGrant(db, schema string) GrantBuilder {
	name, qualifiedName, allTarget := getAllNameAndQualifiedName(db, schema)
	return &AllGrantBuilder{
		name:           name,
		qualifiedName:  qualifiedName,
		allGrantType:   allTableType,
		allGrantTarget: allTarget,
	}
}

// AllViewGrant returns a pointer to a AllGrantBuilder for a view
func AllViewGrant(db, schema string) GrantBuilder {
	name, qualifiedName, allTarget := getAllNameAndQualifiedName(db, schema)
	return &AllGrantBuilder{
		name:           name,
		qualifiedName:  qualifiedName,
		allGrantType:   allViewType,
		allGrantTarget: allTarget,
	}
}

// AllMaterializedViewGrant returns a pointer to a AllGrantBuilder for a view
func AllMaterializedViewGrant(db, schema string) GrantBuilder {
	name, qualifiedName, allTarget := getAllNameAndQualifiedName(db, schema)
	return &AllGrantBuilder{
		name:           name,
		qualifiedName:  qualifiedName,
		allGrantType:   allMaterializedViewType,
		allGrantTarget: allTarget,
	}
}

// // FutureStageGrant returns a pointer to a FutureGrantBuilder for a stage
// func FutureStageGrant(db, schema string) GrantBuilder {
// 	name, qualifiedName, futureTarget := getAllNameAndQualifiedName(db, schema)
// 	return &FutureGrantBuilder{
// 		name:              name,
// 		qualifiedName:     qualifiedName,
// 		futureGrantType:   futureStageType,
// 		futureGrantTarget: futureTarget,
// 	}
// }

// // FutureExternalTableGrant returns a pointer to a FutureGrantBuilder for a external table
// func FutureExternalTableGrant(db, schema string) GrantBuilder {
// 	name, qualifiedName, futureTarget := getAllNameAndQualifiedName(db, schema)
// 	return &FutureGrantBuilder{
// 		name:              name,
// 		qualifiedName:     qualifiedName,
// 		futureGrantType:   futureExternalTableType,
// 		futureGrantTarget: futureTarget,
// 	}
// }

// // FutureFileFormatGrant returns a pointer to a FutureGrantBuilder for a file format
// func FutureFileFormatGrant(db, schema string) GrantBuilder {
// 	name, qualifiedName, futureTarget := getAllNameAndQualifiedName(db, schema)
// 	return &FutureGrantBuilder{
// 		name:              name,
// 		qualifiedName:     qualifiedName,
// 		futureGrantType:   futureFileFormatType,
// 		futureGrantTarget: futureTarget,
// 	}
// }

// // FutureFunctionGrant returns a pointer to a FutureGrantBuilder for a function
// func FutureFunctionGrant(db, schema string) GrantBuilder {
// 	name, qualifiedName, futureTarget := getAllNameAndQualifiedName(db, schema)
// 	return &FutureGrantBuilder{
// 		name:              name,
// 		qualifiedName:     qualifiedName,
// 		futureGrantType:   futureFunctionType,
// 		futureGrantTarget: futureTarget,
// 	}
// }

// // FutureProcedureGrant returns a pointer to a FutureGrantBuilder for a procedure
// func FutureProcedureGrant(db, schema string) GrantBuilder {
// 	name, qualifiedName, futureTarget := getAllNameAndQualifiedName(db, schema)
// 	return &FutureGrantBuilder{
// 		name:              name,
// 		qualifiedName:     qualifiedName,
// 		futureGrantType:   futureProcedureType,
// 		futureGrantTarget: futureTarget,
// 	}
// }

// // FutureSequenceGrant returns a pointer to a FutureGrantBuilder for a sequence
// func FutureSequenceGrant(db, schema string) GrantBuilder {
// 	name, qualifiedName, futureTarget := getAllNameAndQualifiedName(db, schema)
// 	return &FutureGrantBuilder{
// 		name:              name,
// 		qualifiedName:     qualifiedName,
// 		futureGrantType:   futureSequenceType,
// 		futureGrantTarget: futureTarget,
// 	}
// }

// // FutureStreamGrant returns a pointer to a FutureGrantBuilder for a stream
// func FutureStreamGrant(db, schema string) GrantBuilder {
// 	name, qualifiedName, futureTarget := getAllNameAndQualifiedName(db, schema)
// 	return &FutureGrantBuilder{
// 		name:              name,
// 		qualifiedName:     qualifiedName,
// 		futureGrantType:   futureStreamType,
// 		futureGrantTarget: futureTarget,
// 	}
// }

// // FuturePipeGrant returns a pointer to a FutureGrantBuilder for a pipe
// func FuturePipeGrant(db, schema string) GrantBuilder {
// 	name, qualifiedName, futureTarget := getAllNameAndQualifiedName(db, schema)
// 	return &FutureGrantBuilder{
// 		name:              name,
// 		qualifiedName:     qualifiedName,
// 		futureGrantType:   futurePipeType,
// 		futureGrantTarget: futureTarget,
// 	}
// }

// // FutureTaskGrant returns a pointer to a FutureGrantBuilder for a task
// func FutureTaskGrant(db, schema string) GrantBuilder {
// 	name, qualifiedName, futureTarget := getAllNameAndQualifiedName(db, schema)
// 	return &FutureGrantBuilder{
// 		name:              name,
// 		qualifiedName:     qualifiedName,
// 		futureGrantType:   futureTaskType,
// 		futureGrantTarget: futureTarget,
// 	}
// }

// Show returns the SQL that will show all privileges on the grant
func (fgb *AllGrantBuilder) Show() string {
	return fmt.Sprintf(`SHOW FUTURE GRANTS IN %v %v`, fgb.allGrantTarget, fgb.qualifiedName)
}

// AllGrantExecutable abstracts the creation of SQL queries to build future grants for
// different future grant types.
type AllGrantExecutable struct {
	grantName      string
	granteeName    string
	allGrantType   allGrantType
	allGrantTarget allGrantTarget
}

// Role returns a pointer to a FutureGrantExecutable for a role
func (fgb *AllGrantBuilder) Role(n string) GrantExecutable {
	return &AllGrantExecutable{
		granteeName:    n,
		grantName:      fgb.qualifiedName,
		allGrantType:   fgb.allGrantType,
		allGrantTarget: fgb.allGrantTarget,
	}
}

// Share is not implemented because future objects cannot be granted to shares.
func (gb *AllGrantBuilder) Share(n string) GrantExecutable {
	return nil
}

// Grant returns the SQL that will grant future privileges on the grant to the grantee
func (fge *AllGrantExecutable) Grant(p string, w bool, all bool) string {
	var template string
	if w {
		template = `GRANT %v ON ALL %vS IN %v %v TO ROLE "%v" WITH GRANT OPTION`
	} else {
		template = `GRANT %v ON ALL %vS IN %v %v TO ROLE "%v"`
	}
	return fmt.Sprintf(template,
		p, fge.allGrantType, fge.allGrantTarget, fge.grantName, fge.granteeName)
}

// Revoke returns the SQL that will revoke all privileges on the grant from the grantee
func (fge *AllGrantExecutable) Revoke(p string) []string {
	return []string{
		fmt.Sprintf(`REVOKE %v ON ALL %vS IN %v %v FROM ROLE "%v"`,
			p, fge.allGrantType, fge.allGrantTarget, fge.grantName, fge.granteeName),
	}
}

// Show returns the SQL that will show all all grants on the schema
func (fge *AllGrantExecutable) Show() string {
	return fmt.Sprintf(`SHOW ALL GRANTS IN %v %v`, fge.allGrantTarget, fge.grantName)
}
