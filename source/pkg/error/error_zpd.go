package error_zpd

import "errors"

var (
	ErrDBNExists              = errors.New("Database name already exist")
	ErrGetPeerFailed          = errors.New("Get peer context failed")
	ErrClientNoExists         = errors.New("Client does not exists")
	ErrSchemaNoExists         = errors.New("Schema does not exists")
	ErrDoNotFindLeader        = errors.New("Do not find leader")
	ErrConnectionDoesNotReady = errors.New("Connection does not ready")
	ErrServerDoesNotReady     = errors.New("Server does not ready")
	ErrNotLeader              = errors.New("It is not leader")
	ErrAddressServer          = errors.New("Error Address Leader")
	ErrDoNotUseDatabase       = errors.New("Do not use Database")
	ErrTBNNExists             = errors.New("Table name already exist")
	ErrBuildExecutorFail      = errors.New("Build executor fail")
	ErrDuplicateColumn        = errors.New("Table has duplicate column")
	ErrTooManyColumn          = errors.New("Table too many column")
	ErrTableNoExists          = errors.New("Table does not exists")
	ErrHaveNotTable           = errors.New("Have not table to select row")
	ErrColumnNotExist         = errors.New("Column is not exists")
	ErrDifferentTypeColumn    = errors.New("Column is different type")
	ErrNotNull                = errors.New("Value column is not null")
	ErrUnique                 = errors.New("Duplicate value")
	ErrRowIsNotExists         = errors.New("Do not find data")
)
