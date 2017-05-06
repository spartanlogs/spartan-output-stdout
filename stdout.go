package stdout

import (
	"fmt"

	"github.com/spartanlogs/spartan/codecs"
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
	next  outputs.Output
	codec codecs.Codec
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
	if err := config.VerifySettings(options, stdOutConfigSchema); err != nil {
		return err
	}

	options.Set("codec", options.Get("codec").(string))

	return nil
}

// SetNext sets the next Output in line.
func (o *StdOutOutput) SetNext(next outputs.Output) {
	o.next = next
}

// SetCodec sets the codec
func (o *StdOutOutput) SetCodec(c codecs.Codec) {
	o.codec = c
}

// Run processes a batch.
func (o *StdOutOutput) Run(batch []*event.Event) {
	for _, event := range batch {
		if event != nil {
			fmt.Print(string(o.codec.Encode(event)))
		}
	}
	o.next.Run(batch)
}

