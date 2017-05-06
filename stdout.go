package stdout

import (
	"fmt"

	"github.com/lfkeitel/spartan/codecs"
	"github.com/lfkeitel/spartan/config"
	"github.com/lfkeitel/spartan/event"
	"github.com/lfkeitel/spartan/outputs"
	"github.com/lfkeitel/spartan/utils"
)

func init() {
	outputs.Register("stdout", newStdOutOutput)
}

type stdOutConfig struct {
	codec codecs.Codec
}

var stdOutConfigSchema = []config.Setting{
	{
		Name:    "codec",
		Type:    config.String,
		Default: "json",
	},
}

// StdOutOutput prints events to StdOut.
type StdOutOutput struct {
	config *stdOutConfig
	next   outputs.Output
}

func newStdOutOutput(options utils.InterfaceMap) (outputs.Output, error) {
	options = config.CheckOptionsMap(options)
	o := &StdOutOutput{config: &stdOutConfig{}}
	if err := o.setConfig(options); err != nil {
		return nil, err
	}
	return o, nil
}

func (o *StdOutOutput) setConfig(options utils.InterfaceMap) error {
	if err := config.VerifySettings(options, stdOutConfigSchema); err != nil {
		return err
	}

	codec := options.Get("codec").(string)
	c, _ := codecs.New(codec)
	o.config.codec = c

	return nil
}

// SetNext sets the next Output in line.
func (o *StdOutOutput) SetNext(next outputs.Output) {
	o.next = next
}

// Run processes a batch.
func (o *StdOutOutput) Run(batch []*event.Event) {
	for _, event := range batch {
		if event != nil {
			fmt.Printf("%s\n", o.config.codec.Encode(event))
		}
	}
	o.next.Run(batch)
}
