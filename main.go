package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ─── Models ──────────────────────────────────────────────────────────────────

type SpenderEntry struct {
	Rank   int    `json:"rank"`
	Phone  string `json:"phone"`
	Amount int    `json:"amount"`
}

type Section struct {
	Name      string          `json:"name"`
	Type      string          `json:"type"` // "spender" | "lucky"
	MinAmount int             `json:"min_amount,omitempty"`
	Entries   json.RawMessage `json:"entries"`
}

type Event struct {
	ID        int       `json:"id"`
	Title     string    `json:"title"`
	Member    string    `json:"member"`
	Date      string    `json:"date"`
	Period    string    `json:"period"`
	StartDate string    `json:"start_date,omitempty"`
	EndDate   string    `json:"end_date,omitempty"`
	Hashtag   string    `json:"hashtag"`
	Img       string    `json:"img"`
	Sections  []Section `json:"sections"`
	CreatedAt time.Time `json:"created_at"`
}

type Database struct {
	Events []Event `json:"events"`
}

type Stats struct {
	TotalEvents      int               `json:"total_events"`
	HighestAmount    int               `json:"highest_amount"`
	HighestEvent     string            `json:"highest_event"`
	HighestMember    string            `json:"highest_member"`
	MembersCount     int               `json:"members_count"`
	Members          []string          `json:"members"`
	CountEachMember  []CountEachMember `json:"count_each_member"`
	LatestEventDate  string            `json:"latest_event_date"`
	LatestEventTitle string            `json:"latest_event_title"`
	LatestMember     string            `json:"latest_member"`
}

type CountEachMember struct {
	MemberName string `json:"member_name"`
	Count      int    `json:"count"`
}

// ─── Store ───────────────────────────────────────────────────────────────────

const (
	dataFile = "data.json"
	photoDir = "Photo_topspender"
)

var (
	db   Database
	dbMu sync.RWMutex
)

func loadDB() error {
	data, err := os.ReadFile(dataFile)
	if os.IsNotExist(err) {
		db = Database{Events: []Event{}}
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &db)
}

func saveDB() error {
	data, err := json.MarshalIndent(db, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(dataFile, data, 0644)
}

func nextID() int {
	max := 0
	for _, e := range db.Events {
		if e.ID > max {
			max = e.ID
		}
	}
	return max + 1
}

// ─── Stats ───────────────────────────────────────────────────────────────────

func computeStats() Stats {
	dbMu.RLock()
	events := make([]Event, len(db.Events))
	copy(events, db.Events)
	dbMu.RUnlock()

	if len(events) == 0 {
		return Stats{Members: []string{}}
	}

	memberSet := map[string]int{}
	highest := 0
	highestEvent := ""
	highestMember := ""
	// highestEachMember := ""
	latestEvent := events[0]

	for _, ev := range events {
		// log.Println("ev.Member", ev.Member)
		// log.Println("ev.ID", ev.ID)

		memberSet[ev.Member] += 1
		if ev.ID > latestEvent.ID {
			latestEvent = ev
		}
		for _, sec := range ev.Sections {
			if sec.Type != "spender" {
				continue
			}
			var entries []SpenderEntry
			if err := json.Unmarshal(sec.Entries, &entries); err != nil {
				continue
			}
			for _, e := range entries {
				if e.Rank == 1 && e.Amount > highest {
					highest = e.Amount
					highestEvent = ev.Title
					highestMember = ev.Member
				}
			}
			break
		}
	}

	members := make([]string, 0, len(memberSet))
	for m := range memberSet {
		members = append(members, m)
	}
	sort.Strings(members)

	countEachMember := []CountEachMember{}
	for k, i := range memberSet {
		countEachMember = append(countEachMember, CountEachMember{
			MemberName: k,
			Count:      i,
		})
	}

	// log.Println(len(events),
	// 	highest,
	// 	highestEvent,
	// 	highestMember,
	// 	len(members),
	// 	members,
	// 	latestEvent.Date,
	// 	latestEvent.Title,
	// 	latestEvent.Member)

	return Stats{
		TotalEvents:      len(events),
		HighestAmount:    highest,
		HighestEvent:     highestEvent,
		HighestMember:    highestMember,
		MembersCount:     len(members),
		Members:          members,
		CountEachMember:  countEachMember,
		LatestEventDate:  latestEvent.Date,
		LatestEventTitle: latestEvent.Title,
		LatestMember:     latestEvent.Member,
	}
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ─── Handlers ────────────────────────────────────────────────────────────────

// GET  /api/events → { events, stats }
// POST /api/events → create event (multipart/form-data)
func eventsHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		dbMu.RLock()
		events := make([]Event, len(db.Events))
		copy(events, db.Events)
		dbMu.RUnlock()

		sort.Slice(events, func(i, j int) bool {
			di, dj := events[i].EndDate, events[j].EndDate
			if di == "" { di = events[i].Date }
			if dj == "" { dj = events[j].Date }
			ti, _ := time.Parse("02 Jan 2006", di)
			tj, _ := time.Parse("02 Jan 2006", dj)
			return ti.After(tj)
		})
		writeJSON(w, http.StatusOK, map[string]any{
			"events": events,
			"stats":  computeStats(),
		})

	case http.MethodPost:
		createEventHandler(w, r)

	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// DELETE /api/events/{id}
func eventByIDHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	idStr := strings.TrimPrefix(r.URL.Path, "/api/events/")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}

	dbMu.Lock()
	defer dbMu.Unlock()

	idx := -1
	for i, e := range db.Events {
		if e.ID == id {
			idx = i
			break
		}
	}
	if idx == -1 {
		writeError(w, http.StatusNotFound, "event not found")
		return
	}

	db.Events = append(db.Events[:idx], db.Events[idx+1:]...)
	if err := saveDB(); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to save")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// GET /api/stats
func statsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, computeStats())
}

func createEventHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "failed to parse form")
		return
	}

	title := strings.TrimSpace(r.FormValue("title"))
	member := strings.TrimSpace(r.FormValue("member"))
	date := strings.TrimSpace(r.FormValue("date"))
	period := strings.TrimSpace(r.FormValue("period"))
	startDate := strings.TrimSpace(r.FormValue("start_date"))
	endDate := strings.TrimSpace(r.FormValue("end_date"))
	hashtag := strings.TrimSpace(r.FormValue("hashtag"))

	if title == "" || member == "" || date == "" {
		writeError(w, http.StatusBadRequest, "title, member, and date are required")
		return
	}

	sectionsRaw := r.FormValue("sections")
	if sectionsRaw == "" {
		writeError(w, http.StatusBadRequest, "sections are required")
		return
	}
	var sections []Section
	if err := json.Unmarshal([]byte(sectionsRaw), &sections); err != nil {
		writeError(w, http.StatusBadRequest, "invalid sections JSON: "+err.Error())
		return
	}

	imgPath := ""
	file, header, err := r.FormFile("photo")
	if err == nil {
		defer file.Close()
		ext := filepath.Ext(header.Filename)
		if ext == "" {
			ext = ".jpeg"
		}
		filename := fmt.Sprintf("%d%s", time.Now().UnixNano(), ext)
		dst := filepath.Join(photoDir, filename)
		out, err := os.Create(dst)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to save photo")
			return
		}
		defer out.Close()
		if _, err = io.Copy(out, file); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to write photo")
			return
		}
		imgPath = photoDir + "/" + filename
	}

	dbMu.Lock()
	defer dbMu.Unlock()

	ev := Event{
		ID:        nextID(),
		Title:     title,
		Member:    member,
		Date:      date,
		Period:    period,
		StartDate: startDate,
		EndDate:   endDate,
		Hashtag:   hashtag,
		Img:       imgPath,
		Sections:  sections,
		CreatedAt: time.Now(),
	}
	db.Events = append(db.Events, ev)

	if err := saveDB(); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to save event")
		return
	}
	writeJSON(w, http.StatusCreated, ev)
}

// ─── Extract ──────────────────────────────────────────────────────────────────

// Anthropic API request/response shapes (minimal)
type claudeRequest struct {
	Model     string          `json:"model"`
	MaxTokens int             `json:"max_tokens"`
	Messages  []claudeMessage `json:"messages"`
}

type claudeMessage struct {
	Role    string        `json:"role"`
	Content []claudeBlock `json:"content"`
}

type claudeBlock struct {
	Type   string        `json:"type"`
	Text   string        `json:"text,omitempty"`
	Source *claudeSource `json:"source,omitempty"`
}

type claudeSource struct {
	Type      string `json:"type"`
	MediaType string `json:"media_type"`
	Data      string `json:"data"`
}

type claudeResponse struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
}

// POST /api/extract  body: multipart with field "filename" (name inside Photo_topspender/)
//
//	or field "photo" (uploaded file)
func extractHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		writeError(w, http.StatusInternalServerError, "ANTHROPIC_API_KEY not set")
		return
	}

	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "failed to parse form")
		return
	}

	// Read image bytes — either from uploaded file or existing filename
	var imgBytes []byte
	var mediaType string

	file, header, err := r.FormFile("photo")
	if err == nil {
		defer file.Close()
		imgBytes, err = io.ReadAll(file)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to read photo")
			return
		}
		mediaType = mimeFromExt(filepath.Ext(header.Filename))
	} else {
		filename := strings.TrimSpace(r.FormValue("filename"))
		if filename == "" {
			writeError(w, http.StatusBadRequest, "provide 'photo' file or 'filename' field")
			return
		}
		// Sanitise: no path traversal
		filename = filepath.Base(filename)
		imgBytes, err = os.ReadFile(filepath.Join(photoDir, filename))
		if err != nil {
			writeError(w, http.StatusNotFound, "photo not found: "+filename)
			return
		}
		mediaType = mimeFromExt(filepath.Ext(filename))
	}

	b64 := base64.StdEncoding.EncodeToString(imgBytes)

	prompt := `This image shows a top spender leaderboard from a fan-meeting event.
Extract all the data and return ONLY a valid JSON object in this exact shape:
{
  "section_name": "TOP STUDIO",
  "entries": [
    {"rank": 1, "phone": "091-079xxxx", "amount": 77420},
    ...
  ]
}
Rules:
- "section_name": the section title shown in the image (e.g. "TOP STUDIO", "TOP SPENDER").
- "entries": every row visible, sorted by rank ascending.
- "phone": copy the masked phone number exactly as shown.
- "amount": integer, Thai baht, no commas or symbols.
- Return ONLY the JSON object. No markdown, no explanation.`

	reqBody := claudeRequest{
		Model:     "claude-sonnet-4-6",
		MaxTokens: 2048,
		Messages: []claudeMessage{
			{
				Role: "user",
				Content: []claudeBlock{
					{
						Type: "image",
						Source: &claudeSource{
							Type:      "base64",
							MediaType: mediaType,
							Data:      b64,
						},
					},
					{Type: "text", Text: prompt},
				},
			},
		},
	}

	bodyBytes, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest(http.MethodPost, "https://api.anthropic.com/v1/messages", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", apiKey)
	req.Header.Set("anthropic-version", "2023-06-01")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		writeError(w, http.StatusBadGateway, "claude api error: "+err.Error())
		return
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		// Surface the actual API error message
		var apiErr struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		json.Unmarshal(respBytes, &apiErr)
		msg := apiErr.Error.Message
		if msg == "" {
			msg = fmt.Sprintf("claude API status %d: %s", resp.StatusCode, string(respBytes))
		}
		writeError(w, http.StatusBadGateway, msg)
		return
	}

	var claudeResp claudeResponse
	if err := json.Unmarshal(respBytes, &claudeResp); err != nil || len(claudeResp.Content) == 0 {
		writeError(w, http.StatusBadGateway, "failed to parse claude response: "+string(respBytes))
		return
	}

	// Strip markdown code fences Claude sometimes adds despite instructions
	raw := strings.TrimSpace(claudeResp.Content[0].Text)
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)

	// Parse Claude's JSON output
	var extracted struct {
		SectionName string `json:"section_name"`
		Entries     []struct {
			Rank   int    `json:"rank"`
			Phone  string `json:"phone"`
			Amount int    `json:"amount"`
		} `json:"entries"`
	}
	if err := json.Unmarshal([]byte(raw), &extracted); err != nil {
		// Return raw text so caller can inspect
		writeJSON(w, http.StatusOK, map[string]string{"raw": raw})
		return
	}

	// Build a Section ready to drop into an Event
	entries := make([]SpenderEntry, len(extracted.Entries))
	for i, e := range extracted.Entries {
		entries[i] = SpenderEntry{Rank: e.Rank, Phone: e.Phone, Amount: e.Amount}
	}
	entriesJSON, _ := json.Marshal(entries)

	section := Section{
		Name:    extracted.SectionName,
		Type:    "spender",
		Entries: json.RawMessage(entriesJSON),
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"section": section,
	})
}

func mimeFromExt(ext string) string {
	switch strings.ToLower(ext) {
	case ".png":
		return "image/png"
	case ".gif":
		return "image/gif"
	case ".webp":
		return "image/webp"
	default:
		return "image/jpeg"
	}
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	if err := loadDB(); err != nil {
		log.Fatalf("failed to load data.json: %v", err)
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/api/events", eventsHandler)
	mux.HandleFunc("/api/events/", eventByIDHandler)
	mux.HandleFunc("/api/stats", statsHandler)
	mux.HandleFunc("/api/extract", extractHandler)

	// Serve uploaded photos
	mux.Handle("/Photo_topspender/", http.StripPrefix("/Photo_topspender/",
		http.FileServer(http.Dir(photoDir))))

	// Serve frontend from public/
	mux.Handle("/", http.FileServer(http.Dir("public")))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Println("Server running → http://localhost:" + port)
	log.Fatal(http.ListenAndServe(":"+port, cors(mux)))
}
