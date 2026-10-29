package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/romycode/qsrv/pkg/client"
)

const topicUsage = `qctl topic — fan messages out to several queues

  topic list                              list topics and their queues
  topic create NAME [QUEUE...]            create, optionally subscribing queues
  topic info NAME
  topic delete NAME                       its queues and their messages stay
  topic sub NAME QUEUE                    QUEUE gets a copy of each new message
  topic unsub NAME QUEUE
  topic publish [-group G] [-delay D] NAME [BODY|-]
                                          one copy per subscribed queue, atomically
`

func cmdTopic(ctx context.Context, c *client.Client, args []string) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, topicUsage)
		return exitError(2)
	}
	sub, args := args[0], args[1:]
	need := func(n int, usage string) error {
		if len(args) != n {
			return errors.New("usage: qctl topic " + usage)
		}
		return nil
	}
	switch sub {
	case "list":
		ts, err := c.ListTopics(ctx)
		if err != nil {
			return err
		}
		for _, t := range ts {
			fmt.Println(t.Name, t.Queues)
		}
		return nil
	case "create":
		if len(args) < 1 {
			return errors.New("usage: qctl topic create NAME [QUEUE...]")
		}
		t, err := c.CreateTopic(ctx, args[0], args[1:]...)
		if err != nil {
			return err
		}
		return printJSON(t)
	case "info":
		if err := need(1, "info NAME"); err != nil {
			return err
		}
		t, err := c.GetTopic(ctx, args[0])
		if err != nil {
			return err
		}
		return printJSON(t)
	case "delete":
		if err := need(1, "delete NAME"); err != nil {
			return err
		}
		return c.DeleteTopic(ctx, args[0])
	case "sub", "unsub":
		if err := need(2, sub+" NAME QUEUE"); err != nil {
			return err
		}
		fn := c.Subscribe
		if sub == "unsub" {
			fn = c.Unsubscribe
		}
		t, err := fn(ctx, args[0], args[1])
		if err != nil {
			return err
		}
		return printJSON(t)
	case "publish":
		fs := flag.NewFlagSet("topic publish", flag.ExitOnError)
		group := fs.String("group", "", "ordering group id (applies in each queue)")
		delay := fs.Duration("delay", 0, "deliverable only after this long (max 720h)")
		fs.Parse(args)
		if fs.NArg() < 1 || fs.NArg() > 2 {
			return errors.New("usage: qctl topic publish [-group G] [-delay D] NAME [BODY|-]")
		}
		var body []byte
		if fs.NArg() == 2 && fs.Arg(1) != "-" {
			body = []byte(fs.Arg(1))
		} else {
			b, err := io.ReadAll(os.Stdin)
			if err != nil {
				return err
			}
			body = b
		}
		pub, err := c.Publish(ctx, fs.Arg(0), body, client.SendOptions{GroupID: *group, Delay: *delay})
		if err != nil {
			return err
		}
		for _, p := range pub {
			fmt.Println(p.Queue, p.ID)
		}
		return nil
	default:
		fmt.Fprint(os.Stderr, topicUsage)
		return fmt.Errorf("unknown topic command %q", sub)
	}
}
