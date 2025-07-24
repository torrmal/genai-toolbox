// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package util

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/go-playground/validator/v10"
	yaml "github.com/goccy/go-yaml"
	"github.com/googleapis/genai-toolbox/internal/log"
	"github.com/googleapis/genai-toolbox/internal/telemetry"
)

// DecodeJSON decodes a given reader into an interface using the json decoder.
func DecodeJSON(r io.Reader, v interface{}) error {
	defer io.Copy(io.Discard, r) //nolint:errcheck
	d := json.NewDecoder(r)
	// specify JSON numbers should get parsed to json.Number instead of float64 by default.
	// This prevents loss between floats/ints.
	d.UseNumber()
	return d.Decode(v)
}

// ConvertNumbers traverses an interface and converts all json.Number
// instances to int64 or float64.
func ConvertNumbers(data any) (any, error) {
	switch v := data.(type) {
	// If it's a map, recursively convert the values.
	case map[string]any:
		for key, val := range v {
			convertedVal, err := ConvertNumbers(val)
			if err != nil {
				return nil, err
			}
			v[key] = convertedVal
		}
		return v, nil

	// If it's a slice, recursively convert the elements.
	case []any:
		for i, val := range v {
			convertedVal, err := ConvertNumbers(val)
			if err != nil {
				return nil, err
			}
			v[i] = convertedVal
		}
		return v, nil

	// If it's a json.Number, convert it to float or int
	case json.Number:
		// Check for a decimal point to decide the type.
		if strings.Contains(v.String(), ".") {
			return v.Float64()
		}
		return v.Int64()

	// For all other types, return them as is.
	default:
		return data, nil
	}
}

var _ yaml.InterfaceUnmarshalerContext = &DelayedUnmarshaler{}

// DelayedUnmarshaler is struct that saves the provided unmarshal function
// passed to UnmarshalYAML so it can be re-used later once the target interface
// is known.
type DelayedUnmarshaler struct {
	unmarshal func(interface{}) error
}

func (d *DelayedUnmarshaler) UnmarshalYAML(ctx context.Context, unmarshal func(interface{}) error) error {
	d.unmarshal = unmarshal
	return nil
}

func (d *DelayedUnmarshaler) Unmarshal(v interface{}) error {
	if d.unmarshal == nil {
		return fmt.Errorf("nothing to unmarshal")
	}
	return d.unmarshal(v)
}

type contextKey string

// userAgentKey is the key used to store userAgent within context
const userAgentKey contextKey = "userAgent"

// WithUserAgent adds a user agent into the context as a value
func WithUserAgent(ctx context.Context, versionString string) context.Context {
	userAgent := "genai-toolbox/" + versionString
	return context.WithValue(ctx, userAgentKey, userAgent)
}

// UserAgentFromContext retrieves the user agent or return an error
func UserAgentFromContext(ctx context.Context) (string, error) {
	if ua := ctx.Value(userAgentKey); ua != nil {
		return ua.(string), nil
	} else {
		return "", fmt.Errorf("unable to retrieve user agent")
	}
}

func NewStrictDecoder(v interface{}) (*yaml.Decoder, error) {
	b, err := yaml.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("fail to marshal %q: %w", v, err)
	}

	dec := yaml.NewDecoder(
		bytes.NewReader(b),
		yaml.Strict(),
		yaml.Validator(validator.New()),
	)
	return dec, nil
}

// loggerKey is the key used to store logger within context
const loggerKey contextKey = "logger"

// WithLogger adds a logger into the context as a value
func WithLogger(ctx context.Context, logger log.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, logger)
}

// LoggerFromContext retrieves the logger or return an error
func LoggerFromContext(ctx context.Context) (log.Logger, error) {
	if logger, ok := ctx.Value(loggerKey).(log.Logger); ok {
		return logger, nil
	}
	return nil, fmt.Errorf("unable to retrieve logger")
}

const instrumentationKey contextKey = "instrumentation"

// WithInstrumentation adds an instrumentation into the context as a value
func WithInstrumentation(ctx context.Context, instrumentation *telemetry.Instrumentation) context.Context {
	return context.WithValue(ctx, instrumentationKey, instrumentation)
}

// InstrumentationFromContext retrieves the instrumentation or return an error
func InstrumentationFromContext(ctx context.Context) (*telemetry.Instrumentation, error) {
	if instrumentation, ok := ctx.Value(instrumentationKey).(*telemetry.Instrumentation); ok {
		return instrumentation, nil
	}
	return nil, fmt.Errorf("unable to retrieve instrumentation")
}
