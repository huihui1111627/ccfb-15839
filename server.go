package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
)

//go:embed web/index.html
var indexHTML []byte

type Server struct {
	mu  sync.RWMutex
	exp *Experiment
}

func startServer(addr string, exp *Experiment) {
	s := &Server{exp: exp}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})
	http.HandleFunc("/api/report", func(w http.ResponseWriter, r *http.Request) {
		s.mu.RLock()
		defer s.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		json.NewEncoder(w).Encode(s.exp)
	})
	http.HandleFunc("/api/rerun", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		seed, err := strconv.ParseInt(r.URL.Query().Get("seed"), 10, 64)
		if err != nil {
			seed = 42
		}
		exp := RunExperiment(seed)
		s.mu.Lock()
		s.exp = exp
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		json.NewEncoder(w).Encode(exp)
	})

	fmt.Printf("控制台已启动: http://localhost%s\n", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		fmt.Println("HTTP 服务退出:", err)
	}
}
