package chief

import (
	"log"
	"net/http"
)

func ExampleCheckWebPages() {
	handler := func(j Task) {
		url := j.(string)
		resp, err := http.Get(url)

		if err != nil {
			log.Println(err)
		}

		log.Println(resp.Status)
	}

	c := &Master{
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
