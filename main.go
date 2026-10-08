package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
)

// corsMiddleware -- S79 (ADR S0-083, service-ref Phase B suite). Reflete
// l'origine recue dans Access-Control-Allow-Origin uniquement si elle
// figure dans ALLOWED_ORIGINS (liste separee par virgules, vide par
// defaut -- aucun changement de comportement pour un service sans
// service-ref reference vers lui). ALLOWED_ORIGINS est mis a jour et le
// pod redemarre par dxp-serve des qu'un service tiers declare une
// dependance de nature browser vers ce service (propagation automatique,
// pas d'intervention manuelle du tl).
func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	allowed := map[string]bool{}
	for _, o := range strings.Split(os.Getenv("ALLOWED_ORIGINS"), ",") {
		o = strings.TrimSpace(o)
		if o != "" {
			allowed[o] = true
		}
	}
	return func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && allowed[origin] {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next(w, r)
	}
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	http.HandleFunc("/", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"service": "atlas-avis-api", "status": "ok", "platform": "DxP"})
	}))
	http.HandleFunc("/health", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))
	log.Printf("atlas-avis-api running on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
