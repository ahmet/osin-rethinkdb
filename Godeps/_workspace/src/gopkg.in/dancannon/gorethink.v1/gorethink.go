package gorethink

import (
	"reflect"

	"github.com/ahmet/osin-rethinkdb/Godeps/_workspace/src/github.com/Sirupsen/logrus"

	"github.com/ahmet/osin-rethinkdb/Godeps/_workspace/src/github.com/dancannon/gorethink/encoding"
)

var (
	Log *logrus.Logger
)

func init() {
	// Set encoding package
	encoding.IgnoreType(reflect.TypeOf(Term{}))

	Log = logrus.New()
}

// SetVerbose allows the driver logging level to be set. If true is passed then
// the log level is set to Debug otherwise it defaults to Info.
func SetVerbose(verbose bool) {
	if verbose {
		Log.Level = logrus.DebugLevel
		return
	}

	Log.Level = logrus.InfoLevel
}
