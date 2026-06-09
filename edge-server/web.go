package main

import (
	_ "embed"
	"net/http"
)

//go:embed dashboard.html
var dashboardHTML string

// setupRoutes configures web UI routes
func (web *WebServer) setupRoutes() {
}

// dashboard serves the cluster dashboard HTML page
func (web *WebServer) dashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(dashboardHTML))
}

