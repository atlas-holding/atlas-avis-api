package main

// atlas-avis-api -- Service API (Golden Path Go)
// Catalogue produits et avis clients de l'enseigne Atlas Argan.
//
// Stockage :
//   - PostgreSQL si les identifiants de la base sont presents (secret
//     dxp-db-atlas-avis-db, cree par le Golden Path PostgreSQL) ;
//   - sinon memoire (les donnees reviennent au jeu de depart a chaque
//     redemarrage du pod).
// Le schema et le jeu de depart sont crees automatiquement au demarrage.

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

type Produit struct {
	ID          int     `json:"id"`
	Nom         string  `json:"nom"`
	Categorie   string  `json:"categorie"`
	Prix        float64 `json:"prix"`
	NbAvis      int     `json:"nb_avis"`
	NoteMoyenne float64 `json:"note_moyenne"`
}

type Analyse struct {
	Sentiment       string   `json:"sentiment"`
	Themes          []string `json:"themes"`
	ReponseSuggeree string   `json:"reponse_suggeree"`
}

type Avis struct {
	ID        int      `json:"id"`
	ProduitID int      `json:"produit_id"`
	Client    string   `json:"client"`
	Note      int      `json:"note"`
	Texte     string   `json:"texte"`
	Date      string   `json:"date"`
	Analyse   *Analyse `json:"analyse,omitempty"`
}

var errNotFound = errors.New("introuvable")

// ---------------------------------------------------------------- jeu de depart

var seedProduits = []Produit{
	{ID: 1, Nom: "Huile d'argan bio 100 ml", Categorie: "Soin visage", Prix: 189},
	{ID: 2, Nom: "Savon noir à l'eucalyptus", Categorie: "Hammam", Prix: 59},
	{ID: 3, Nom: "Ghassoul de l'Atlas", Categorie: "Hammam", Prix: 45},
	{ID: 4, Nom: "Eau de rose de Kelâa", Categorie: "Soin visage", Prix: 79},
}

var seedAvis = []Avis{
	{ProduitID: 1, Client: "Salma B.", Note: 5, Texte: "Huile d'excellente qualité, très bonne odeur et la peau est vraiment plus douce après deux semaines.", Date: "2026-10-02"},
	{ProduitID: 1, Client: "Karim E.", Note: 2, Texte: "Produit correct mais livré avec cinq jours de retard et le flacon avait coulé dans le colis. Déçu pour ce prix.", Date: "2026-10-05"},
	{ProduitID: 1, Client: "Nadia T.", Note: 4, Texte: "Bonne huile, un peu chère par rapport à ce qu'on trouve au souk mais la qualité bio se sent.", Date: "2026-10-07"},
	{ProduitID: 2, Client: "Youssef A.", Note: 5, Texte: "Le vrai savon noir comme au hammam de mon quartier. Parfait avec le gant kessa.", Date: "2026-10-01"},
	{ProduitID: 2, Client: "Leila M.", Note: 1, Texte: "Odeur d'eucalyptus beaucoup trop forte et le pot reçu était à moitié vide. Le service client ne répond pas.", Date: "2026-10-06"},
	{ProduitID: 3, Client: "Hind R.", Note: 4, Texte: "Ghassoul fin et sans grumeaux, très efficace pour les cheveux. Livraison rapide.", Date: "2026-10-03"},
	{ProduitID: 4, Client: "Omar Z.", Note: 3, Texte: "Eau de rose agréable mais le spray s'est cassé au bout d'une semaine.", Date: "2026-10-04"},
	{ProduitID: 4, Client: "Sara K.", Note: 5, Texte: "Parfum naturel et délicat, je l'utilise matin et soir. Je recommande.", Date: "2026-10-08"},
}

// ---------------------------------------------------------------- stockage

type Store interface {
	Kind() string
	Ping() error
	Produits() ([]Produit, error)
	ProduitExiste(id int) (bool, error)
	ListeAvis(produitID int) ([]Avis, error)
	GetAvis(id int) (Avis, string, error) // avis + nom du produit
	AjoutAvis(a Avis) (Avis, error)
	SetAnalyse(id int, an Analyse) (Avis, error)
}

// --- memoire

type memStore struct {
	mu     sync.Mutex
	avis   []Avis
	nextID int
}

func newMemStore() *memStore {
	m := &memStore{nextID: 1}
	for _, a := range seedAvis {
		a.ID = m.nextID
		m.nextID++
		m.avis = append(m.avis, a)
	}
	return m
}

func (m *memStore) Kind() string { return "memoire" }
func (m *memStore) Ping() error  { return nil }

func (m *memStore) Produits() ([]Produit, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Produit, len(seedProduits))
	copy(out, seedProduits)
	for i := range out {
		n, total := 0, 0
		for _, a := range m.avis {
			if a.ProduitID == out[i].ID {
				n++
				total += a.Note
			}
		}
		out[i].NbAvis = n
		if n > 0 {
			out[i].NoteMoyenne = arrondi(float64(total) / float64(n))
		}
	}
	return out, nil
}

func (m *memStore) ProduitExiste(id int) (bool, error) {
	for _, p := range seedProduits {
		if p.ID == id {
			return true, nil
		}
	}
	return false, nil
}

func (m *memStore) ListeAvis(pid int) ([]Avis, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []Avis{}
	for i := len(m.avis) - 1; i >= 0; i-- {
		if pid == 0 || m.avis[i].ProduitID == pid {
			out = append(out, m.avis[i])
		}
	}
	return out, nil
}

func (m *memStore) GetAvis(id int) (Avis, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, a := range m.avis {
		if a.ID == id {
			for _, p := range seedProduits {
				if p.ID == a.ProduitID {
					return a, p.Nom, nil
				}
			}
			return a, "", nil
		}
	}
	return Avis{}, "", errNotFound
}

func (m *memStore) AjoutAvis(a Avis) (Avis, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	a.ID = m.nextID
	m.nextID++
	a.Date = time.Now().UTC().Format("2006-01-02")
	a.Analyse = nil
	m.avis = append(m.avis, a)
	return a, nil
}

func (m *memStore) SetAnalyse(id int, an Analyse) (Avis, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.avis {
		if m.avis[i].ID == id {
			m.avis[i].Analyse = &an
			return m.avis[i], nil
		}
	}
	return Avis{}, errNotFound
}

// --- PostgreSQL

type pgStore struct{ db *sql.DB }

const schema = `
CREATE TABLE IF NOT EXISTS produits (
  id        INTEGER PRIMARY KEY,
  nom       TEXT NOT NULL,
  categorie TEXT NOT NULL,
  prix      NUMERIC(10,2) NOT NULL
);
CREATE TABLE IF NOT EXISTS avis (
  id               SERIAL PRIMARY KEY,
  produit_id       INTEGER NOT NULL REFERENCES produits(id),
  client           TEXT NOT NULL DEFAULT '',
  note             INTEGER NOT NULL CHECK (note BETWEEN 1 AND 5),
  texte            TEXT NOT NULL,
  date_avis        DATE NOT NULL DEFAULT CURRENT_DATE,
  sentiment        TEXT,
  themes           TEXT,
  reponse_suggeree TEXT
);`

func newPgStore(dsn string) (*pgStore, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(5)
	// la base peut demarrer apres l'API : on attend jusqu'a 60 s
	for i := 0; ; i++ {
		if err = db.Ping(); err == nil {
			break
		}
		if i == 30 {
			return nil, err
		}
		log.Printf("base pas encore prete (%v), nouvel essai dans 2 s", err)
		time.Sleep(2 * time.Second)
	}
	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("schema: %w", err)
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM produits`).Scan(&n); err != nil {
		return nil, err
	}
	if n == 0 {
		log.Printf("base vide : chargement du jeu de depart")
		for _, p := range seedProduits {
			if _, err := db.Exec(`INSERT INTO produits (id, nom, categorie, prix) VALUES ($1,$2,$3,$4)`, p.ID, p.Nom, p.Categorie, p.Prix); err != nil {
				return nil, err
			}
		}
		for _, a := range seedAvis {
			if _, err := db.Exec(`INSERT INTO avis (produit_id, client, note, texte, date_avis) VALUES ($1,$2,$3,$4,$5)`, a.ProduitID, a.Client, a.Note, a.Texte, a.Date); err != nil {
				return nil, err
			}
		}
	}
	return &pgStore{db: db}, nil
}

func (s *pgStore) Kind() string { return "postgresql" }
func (s *pgStore) Ping() error  { return s.db.Ping() }

func (s *pgStore) Produits() ([]Produit, error) {
	rows, err := s.db.Query(`
SELECT p.id, p.nom, p.categorie, p.prix::float8, count(a.id), COALESCE(avg(a.note), 0)::float8
FROM produits p LEFT JOIN avis a ON a.produit_id = p.id
GROUP BY p.id ORDER BY p.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Produit{}
	for rows.Next() {
		var p Produit
		if err := rows.Scan(&p.ID, &p.Nom, &p.Categorie, &p.Prix, &p.NbAvis, &p.NoteMoyenne); err != nil {
			return nil, err
		}
		p.NoteMoyenne = arrondi(p.NoteMoyenne)
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *pgStore) ProduitExiste(id int) (bool, error) {
	var n int
	err := s.db.QueryRow(`SELECT count(*) FROM produits WHERE id = $1`, id).Scan(&n)
	return n > 0, err
}

const avisCols = `a.id, a.produit_id, a.client, a.note, a.texte, to_char(a.date_avis, 'YYYY-MM-DD'), a.sentiment, a.themes, a.reponse_suggeree`

func scanAvis(sc interface{ Scan(...any) error }, extra ...any) (Avis, error) {
	var a Avis
	var sentiment, themes, reponse sql.NullString
	dest := append([]any{&a.ID, &a.ProduitID, &a.Client, &a.Note, &a.Texte, &a.Date, &sentiment, &themes, &reponse}, extra...)
	if err := sc.Scan(dest...); err != nil {
		return a, err
	}
	if sentiment.Valid {
		an := Analyse{Sentiment: sentiment.String, ReponseSuggeree: reponse.String, Themes: []string{}}
		json.Unmarshal([]byte(themes.String), &an.Themes)
		a.Analyse = &an
	}
	return a, nil
}

func (s *pgStore) ListeAvis(pid int) ([]Avis, error) {
	rows, err := s.db.Query(`SELECT `+avisCols+` FROM avis a WHERE $1 = 0 OR a.produit_id = $1 ORDER BY a.id DESC`, pid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Avis{}
	for rows.Next() {
		a, err := scanAvis(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *pgStore) GetAvis(id int) (Avis, string, error) {
	var nom string
	a, err := scanAvis(s.db.QueryRow(`SELECT `+avisCols+`, p.nom FROM avis a JOIN produits p ON p.id = a.produit_id WHERE a.id = $1`, id), &nom)
	if errors.Is(err, sql.ErrNoRows) {
		return a, "", errNotFound
	}
	return a, nom, err
}

func (s *pgStore) AjoutAvis(a Avis) (Avis, error) {
	var id int
	if err := s.db.QueryRow(`INSERT INTO avis (produit_id, client, note, texte) VALUES ($1,$2,$3,$4) RETURNING id`, a.ProduitID, a.Client, a.Note, a.Texte).Scan(&id); err != nil {
		return Avis{}, err
	}
	out, _, err := s.GetAvis(id)
	return out, err
}

func (s *pgStore) SetAnalyse(id int, an Analyse) (Avis, error) {
	themes, _ := json.Marshal(an.Themes)
	res, err := s.db.Exec(`UPDATE avis SET sentiment = $2, themes = $3, reponse_suggeree = $4 WHERE id = $1`, id, an.Sentiment, string(themes), an.ReponseSuggeree)
	if err != nil {
		return Avis{}, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return Avis{}, errNotFound
	}
	out, _, err := s.GetAvis(id)
	return out, err
}

// ---------------------------------------------------------------- HTTP

var store Store

func arrondi(f float64) float64 { return float64(int(f*10+0.5)) / 10 }

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

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, err error) {
	if errors.Is(err, errNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "avis introuvable"})
		return
	}
	log.Printf("erreur stockage: %v", err)
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "base de donnees indisponible"})
}

// GET /produits
func handleProduits(w http.ResponseWriter, r *http.Request) {
	p, err := store.Produits()
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

// GET  /avis?produit=ID -- avis d'un produit (plus recent en premier)
// POST /avis            -- {produit_id, client, note, texte}
func handleAvisListe(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		pid, _ := strconv.Atoi(r.URL.Query().Get("produit"))
		l, err := store.ListeAvis(pid)
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, http.StatusOK, l)
	case http.MethodPost:
		var a Avis
		if err := json.NewDecoder(r.Body).Decode(&a); err != nil || strings.TrimSpace(a.Texte) == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "texte de l'avis obligatoire"})
			return
		}
		ok, err := store.ProduitExiste(a.ProduitID)
		if err != nil {
			fail(w, err)
			return
		}
		if !ok {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "produit inconnu"})
			return
		}
		if a.Note < 1 || a.Note > 5 {
			a.Note = 3
		}
		out, err := store.AjoutAvis(a)
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, out)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "methode non supportee"})
	}
}

// GET /avis/{id}           -- un avis, avec le nom du produit
// PUT /avis/{id}/analyse   -- enregistre l'analyse IA
func handleAvis(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/avis/"), "/"), "/")
	id, err := strconv.Atoi(parts[0])
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "id invalide"})
		return
	}
	if len(parts) == 2 && parts[1] == "analyse" && r.Method == http.MethodPut {
		var an Analyse
		if err := json.NewDecoder(r.Body).Decode(&an); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "analyse invalide"})
			return
		}
		if an.Themes == nil {
			an.Themes = []string{}
		}
		out, err := store.SetAnalyse(id, an)
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, http.StatusOK, out)
		return
	}
	if len(parts) == 1 && r.Method == http.MethodGet {
		a, nom, err := store.GetAvis(id)
		if err != nil {
			fail(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"avis": a, "produit": nom})
		return
	}
	writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "methode non supportee"})
}

// dsn -- construit la chaine de connexion a partir du secret de la base.
// Hote : nom complet du service dans son namespace (la base vit dans
// atlas-avis-db-dev, l'API dans atlas-avis-api-dev).
func dsn() string {
	pw := os.Getenv("POSTGRES_PASSWORD")
	if pw == "" {
		return ""
	}
	host := os.Getenv("DB_HOST")
	if host == "" {
		host = "atlas-avis-db.atlas-avis-db-dev.svc.cluster.local"
	}
	user := os.Getenv("POSTGRES_USER")
	if user == "" {
		user = "postgres"
	}
	name := os.Getenv("POSTGRES_DB")
	if name == "" {
		name = "atlas-avis-db"
	}
	return fmt.Sprintf("host=%s port=5432 user=%s password='%s' dbname=%s sslmode=disable connect_timeout=5", host, user, strings.ReplaceAll(pw, "'", `\'`), name)
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	if d := dsn(); d != "" {
		pg, err := newPgStore(d)
		if err != nil {
			log.Fatalf("connexion PostgreSQL impossible: %v", err)
		}
		store = pg
	} else {
		store = newMemStore()
	}
	log.Printf("stockage: %s", store.Kind())

	http.HandleFunc("/", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"service": "atlas-avis-api", "status": "ok", "platform": "DxP", "stockage": store.Kind(), "routes": []string{"GET /produits", "GET /avis?produit={id}", "POST /avis", "GET /avis/{id}", "PUT /avis/{id}/analyse"}})
	}))
	http.HandleFunc("/health", corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		base := "ok"
		if err := store.Ping(); err != nil {
			base = "indisponible"
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "stockage": store.Kind(), "base": base})
	}))
	http.HandleFunc("/produits", corsMiddleware(handleProduits))
	http.HandleFunc("/avis", corsMiddleware(handleAvisListe))
	http.HandleFunc("/avis/", corsMiddleware(handleAvis))
	log.Printf("atlas-avis-api running on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
