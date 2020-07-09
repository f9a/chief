# Chief [![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/f9a/chief)

Master Chief handles task with a worker pool. Chief is ready to handle 100k of tasks in a performaned way.

## Example

```go
package main

func main() {
	handler := func(j chief.Task) {
		url := j.(string)
		resp, err := http.Get(url)

		if err != nil {
			log.Println(err)
		}

		log.Println(resp.Status)
	}

	c := &chief.Chief{
		NumWorkers: 2,
		Handler:    handler,
	}

	err := c.Start()
	if err != nil {
		log.Fatal(err)
	}

	urls := []string{
		"http://heise.de",
		"http://blog.fefe.de",
	}
	for _, url := range urls {
		c.Queue(url)
	}

	err = c.Stop()
	if err != nil {
		log.Fatal(err)
	}
}
```
