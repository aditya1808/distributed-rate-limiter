package validations

import (
	"errors"
	"reflect"
	"strings"
	"sync"

	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/validator/v10"

	"github.com/aditya1808/distributed-rate-limiter/constants"
	"github.com/aditya1808/distributed-rate-limiter/types"
)

var (
	once        sync.Once
	registerErr error
)

// Register installs custom validators on gin's validator engine. Safe to call
// more than once.
func Register() error {
	once.Do(func() {
		// Reject unknown JSON fields so typos fail loudly instead of being ignored.
		binding.EnableDecoderDisallowUnknownFields = true

		v, ok := binding.Validator.Engine().(*validator.Validate)
		if !ok {
			registerErr = errors.New("gin validator engine is not go-playground/validator")
			return
		}
		v.RegisterTagNameFunc(fieldName)
		if err := v.RegisterValidation("identifier", isIdentifier); err != nil {
			registerErr = err
			return
		}
		registerErr = v.RegisterValidation("tenant_selector", isTenantSelector)
	})
	return registerErr
}

// fieldName reports fields by their wire name (json or uri tag) in errors.
func fieldName(f reflect.StructField) string {
	for _, tag := range []string{"json", "uri", "form"} {
		name := strings.SplitN(f.Tag.Get(tag), ",", 2)[0]
		if name == "-" {
			return ""
		}
		if name != "" {
			return name
		}
	}
	return f.Name
}

func isIdentifier(fl validator.FieldLevel) bool {
	return types.IdentifierPattern.MatchString(fl.Field().String())
}

func isTenantSelector(fl validator.FieldLevel) bool {
	s := fl.Field().String()
	return s == constants.DefaultTenant || types.IdentifierPattern.MatchString(s)
}
