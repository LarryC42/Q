package q

import (
	"os"
)

var appName string = BaseFilename(os.Args[0])
var appID string = NewID()

// AppID is the unique ID of this instance of the application
func AppID() string {
	return appID
}

// AppName returns the name of this application
func AppName() string {
	return appName
}

// SetAppName sets the application name, default name is the executable name
func SetAppName(name string) Option {
	return func(t *Options) {
		appName = name
	}
}
