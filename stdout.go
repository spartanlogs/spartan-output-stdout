package stdout

import (
	"fmt"

	"github.com/spartanlogs/spartan/config"
	"github.com/spartanlogs/spartan/event"
	"github.com/spartanlogs/spartan/outputs"
	"github.com/spartanlogs/spartan/utils"
)

func init() {
	outputs.Register("stdout", newStdOutOutput)
}

var stdOutConfigSchema = []config.Setting{
	{
		Name:    "codec",
		Type:    config.String,
		Default: "lines",
	},
}

// StdOutOutput prints events to StdOut.
type StdOutOutput struct {
	outputs.BaseOutput
}

func newStdOutOutput(options utils.InterfaceMap) (outputs.Output, error) {
	options = config.CheckOptionsMap(options)
	o := &StdOutOutput{}
	if err := o.setConfig(options); err != nil {
		return nil, err
	}
	return o, nil
}

func (o *StdOutOutput) setConfig(options utils.InterfaceMap) error {
	var err error
	options, err = config.VerifySettings(options, stdOutConfigSchema)
	if err != nil {
		return err
	}

	options.Set("codec", options.Get("codec").(string))

	return nil
}

// Run processes a batch.
func (o *StdOutOutput) Run(batch []*event.Event) {
	for _, event := range batch {
		if event != nil {
			fmt.Print(string(o.Codec.Encode(event)))
		}
	}
	o.Next.Run(batch)
}
