// Command report turns CI outputs into Markdown fragments for the PR comment.
// It lives under .github so `./...` never builds or tests it.
//
//	report tests    -title "SQLite WASM" test.json
//	report coverage -head coverage.out [-base base.out] [-module github.com/romycode/qsrv]
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fail("usage: report tests|coverage ...")
	}
	var err error
	switch os.Args[1] {
	case "tests":
		err = tests(os.Args[2:])
	case "coverage":
		err = coverage(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fail(err.Error())
	}
}

func fail(msg string) {
	fmt.Fprintln(os.Stderr, "report:", msg)
	os.Exit(1)
}

// ---- tests (go test -json) ----

type event struct {
	Action  string
	Package string
	Test    string
	Elapsed float64
	Output  string
}

func tests(args []string) error {
	fs := flag.NewFlagSet("tests", flag.ExitOnError)
	title := fs.String("title", "Tests", "section title")
	fs.Parse(args)
	if fs.NArg() != 1 {
		return fmt.Errorf("tests: want one go test -json file")
	}
	f, err := os.Open(fs.Arg(0))
	if err != nil {
		return err
	}
	defer f.Close()

	var pass, failN, skip int
	var elapsed float64
	failed := map[string][]string{} // "pkg.Test" -> output lines
	output := map[string][]string{}
	failedPkg := map[string]bool{}
	var buildErrs []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for sc.Scan() {
		var e event
		if json.Unmarshal(sc.Bytes(), &e) != nil {
			if line := strings.TrimSpace(sc.Text()); line != "" {
				buildErrs = append(buildErrs, line) // compiler errors are not JSON
			}
			continue
		}
		pkg := path.Base(e.Package)
		key := pkg + "." + e.Test
		switch e.Action {
		case "build-output":
			buildErrs = append(buildErrs, strings.TrimRight(e.Output, "\n"))
		case "output":
			output[key] = append(output[key], e.Output)
		case "pass", "fail", "skip":
			if e.Test == "" { // package result
				elapsed += e.Elapsed
				if e.Action == "fail" && !failedPkg[pkg] {
					failed[pkg+" (package: build, panic, timeout or TestMain)"] = output[key]
				}
				continue
			}
			switch e.Action {
			case "pass":
				pass++
			case "skip":
				skip++
			case "fail":
				failN++
				failedPkg[pkg] = true
				failed[key] = output[key]
			}
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}

	icon := "✅"
	if failN > 0 || len(failed) > 0 || len(buildErrs) > 0 {
		icon = "❌"
	}
	fmt.Printf("#### %s %s\n\n", icon, *title)
	fmt.Printf("%d passed · %d failed · %d skipped · %s package time\n\n",
		pass, failN, skip, (time.Duration(elapsed * float64(time.Second))).Round(100*time.Millisecond))
	for _, name := range slices.Sorted(maps.Keys(failed)) {
		fmt.Printf("<details><summary>❌ <code>%s</code></summary>\n\n```\n%s```\n</details>\n\n", name, tail(failed[name], 40))
	}
	if len(buildErrs) > 0 {
		fmt.Printf("<details><summary>❌ build errors</summary>\n\n```\n%s\n```\n</details>\n\n", strings.Join(buildErrs, "\n"))
	}
	return nil
}

func tail(lines []string, n int) string {
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "")
}

// ---- coverage ----

type block struct{ stmts, hit int }

// profile maps file -> block position -> statements/covered. Blocks may
// repeat across test binaries (-coverpkg); a block is covered if any run hit it.
func readProfile(name string) (map[string]map[string]*block, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseProfile(f)
}

func parseProfile(r io.Reader) (map[string]map[string]*block, error) {
	out := map[string]map[string]*block{}
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "mode:") || line == "" {
			continue
		}
		// file.go:12.3,14.5 2 1
		colon := strings.LastIndex(line, ":")
		fields := strings.Fields(line[colon+1:])
		if colon < 0 || len(fields) != 3 {
			return nil, fmt.Errorf("bad profile line %q", line)
		}
		stmts, err1 := strconv.Atoi(fields[1])
		count, err2 := strconv.Atoi(fields[2])
		if err1 != nil || err2 != nil {
			return nil, fmt.Errorf("bad profile line %q", line)
		}
		file := line[:colon]
		if out[file] == nil {
			out[file] = map[string]*block{}
		}
		b := out[file][fields[0]]
		if b == nil {
			b = &block{stmts: stmts}
			out[file][fields[0]] = b
		}
		if count > 0 {
			b.hit = 1
		}
	}
	return out, sc.Err()
}

type cov struct{ stmts, covered int }

func (c cov) pct() float64 {
	if c.stmts == 0 {
		return 0
	}
	return 100 * float64(c.covered) / float64(c.stmts)
}

func byPackage(p map[string]map[string]*block, module string) (map[string]cov, cov) {
	pkgs := map[string]cov{}
	var total cov
	for file, blocks := range p {
		pkg := strings.TrimPrefix(strings.TrimPrefix(path.Dir(file), module), "/")
		if pkg == "" {
			pkg = "."
		}
		c := pkgs[pkg]
		for _, b := range blocks {
			c.stmts += b.stmts
			total.stmts += b.stmts
			if b.hit > 0 {
				c.covered += b.stmts
				total.covered += b.stmts
			}
		}
		pkgs[pkg] = c
	}
	return pkgs, total
}

func coverage(args []string) error {
	fs := flag.NewFlagSet("coverage", flag.ExitOnError)
	head := fs.String("head", "", "coverage profile of this change")
	base := fs.String("base", "", "coverage profile of the base branch (optional)")
	module := fs.String("module", "github.com/romycode/qsrv", "module path to strip")
	fs.Parse(args)
	if *head == "" {
		return fmt.Errorf("coverage: -head is required")
	}
	hp, err := readProfile(*head)
	if err != nil {
		return err
	}
	hPkgs, hTotal := byPackage(hp, *module)

	var bPkgs map[string]cov
	var bTotal cov
	hasBase := false
	if *base != "" {
		if bp, err := readProfile(*base); err == nil && len(bp) > 0 {
			bPkgs, bTotal = byPackage(bp, *module)
			hasBase = true
		}
	}

	delta := func(h, b cov, ok bool) string {
		if !hasBase || !ok {
			return "new"
		}
		d := h.pct() - b.pct()
		switch {
		case d > 0.05:
			return fmt.Sprintf("🟢 +%.1f", d)
		case d < -0.05:
			return fmt.Sprintf("🔴 %.1f", d)
		}
		return "="
	}

	fmt.Printf("#### 📊 Coverage: %.1f%%", hTotal.pct())
	if hasBase {
		fmt.Printf(" (%s vs base %.1f%%)", delta(hTotal, bTotal, true), bTotal.pct())
	}
	fmt.Printf("\n\n%d/%d statements covered across all packages (`-coverpkg=./...`).\n\n", hTotal.covered, hTotal.stmts)
	fmt.Print("<details><summary>By package</summary>\n\n")
	fmt.Println("| Package | Coverage | Δ | Statements |")
	fmt.Println("|---|---:|---:|---:|")
	for _, pkg := range slices.Sorted(maps.Keys(hPkgs)) {
		h := hPkgs[pkg]
		b, ok := bPkgs[pkg]
		fmt.Printf("| `%s` | %.1f%% | %s | %d/%d |\n", pkg, h.pct(), delta(h, b, ok), h.covered, h.stmts)
	}
	fmt.Print("\n</details>\n\n")
	return nil
}
