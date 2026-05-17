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

	"github.com/joho/godotenv"
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
	ID         int       `json:"id"`
	Title      string    `json:"title"`
	Member     string    `json:"member"`
	Date       string    `json:"date"`
	Period     string    `json:"period"`
	StartDate  string    `json:"start_date,omitempty"`
	EndDate    string    `json:"end_date,omitempty"`
	Hashtag    string    `json:"hashtag"`
	Ref        string    `json:"ref,omitempty"`
	Img        string    `json:"img"`
	CoverPhoto string    `json:"cover_photo"`
	Sections   []Section `json:"sections"`
	CreatedAt  time.Time `json:"created_at"`
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
	MembersStats     []MemberStat      `json:"members_stats"`
	LatestEventDate  string            `json:"latest_event_date"`
	LatestEventTitle string            `json:"latest_event_title"`
	LatestMember     string            `json:"latest_member"`
}

type CountEachMember struct {
	MemberName string `json:"member_name"`
	Count      int    `json:"count"`
}

type MemberStat struct {
	MemberName string `json:"member_name"`
	EventCount int    `json:"event_count"`
	MaxTop1    int    `json:"max_top1"`
	MinLast    int    `json:"min_last"`
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

	// Per-member max top1 / min last entry across all events
	type memberAgg struct {
		count   int
		maxTop1 int
		minLast int // starts at -1 (unset)
	}
	agg := map[string]*memberAgg{}
	for _, ev := range events {
		a, ok := agg[ev.Member]
		if !ok {
			a = &memberAgg{minLast: -1}
			agg[ev.Member] = a
		}
		a.count++
		for _, sec := range ev.Sections {
			if sec.Type != "spender" {
				continue
			}
			var entries []SpenderEntry
			if err := json.Unmarshal(sec.Entries, &entries); err != nil {
				break
			}
			if len(entries) == 0 {
				break
			}
			sort.Slice(entries, func(i, j int) bool { return entries[i].Rank < entries[j].Rank })
			top1 := entries[0].Amount
			last := entries[len(entries)-1].Amount
			if top1 > a.maxTop1 {
				a.maxTop1 = top1
			}
			if a.minLast == -1 || last < a.minLast {
				a.minLast = last
			}
			break
		}
	}
	membersStats := []MemberStat{}
	for name, a := range agg {
		minLast := a.minLast
		if minLast == -1 {
			minLast = 0
		}
		membersStats = append(membersStats, MemberStat{
			MemberName: name,
			EventCount: a.count,
			MaxTop1:    a.maxTop1,
			MinLast:    minLast,
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
		MembersStats:     membersStats,
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
			if di == "" {
				di = events[i].Date
			}
			if dj == "" {
				dj = events[j].Date
			}
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
	ref := strings.TrimSpace(r.FormValue("ref"))
	coverPhoto := strings.TrimSpace(r.FormValue("cover_photo"))

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
	if imgPath == "" {
		imgPath = strings.TrimSpace(r.FormValue("img_url"))
	}

	dbMu.Lock()
	defer dbMu.Unlock()

	ev := Event{
		ID:         nextID(),
		Title:      title,
		Member:     member,
		Date:       date,
		Period:     period,
		StartDate:  startDate,
		EndDate:    endDate,
		Hashtag:    hashtag,
		Ref:        ref,
		Img:        imgPath,
		CoverPhoto: coverPhoto,
		Sections:   sections,
		CreatedAt:  time.Now(),
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

// OpenAI API request/response shapes (minimal)
type gptRequest struct {
	Model     string       `json:"model"`
	MaxTokens int          `json:"max_tokens"`
	Messages  []gptMessage `json:"messages"`
}

type gptMessage struct {
	Role    string     `json:"role"`
	Content []gptBlock `json:"content"`
}

type gptBlock struct {
	Type     string       `json:"type"`
	Text     string       `json:"text,omitempty"`
	ImageURL *gptImageURL `json:"image_url,omitempty"`
}

type gptImageURL struct {
	URL string `json:"url"`
}

type gptResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

// readImageFromRequest reads image bytes from multipart "photo" file, "img_url", or "filename".
func readImageFromRequest(r *http.Request) (imgBytes []byte, mediaType string, errMsg string, status int) {
	file, header, err := r.FormFile("photo")
	if err == nil {
		defer file.Close()
		imgBytes, err = io.ReadAll(file)
		if err != nil {
			return nil, "", "failed to read photo", http.StatusInternalServerError
		}
		return imgBytes, mimeFromExt(filepath.Ext(header.Filename)), "", 0
	}

	if imgURL := strings.TrimSpace(r.FormValue("img_url")); imgURL != "" {
		resp, fetchErr := http.Get(imgURL)
		if fetchErr != nil {
			return nil, "", "failed to fetch image: " + fetchErr.Error(), http.StatusBadGateway
		}
		defer resp.Body.Close()
		imgBytes, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, "", "failed to read image response", http.StatusBadGateway
		}
		ct := resp.Header.Get("Content-Type")
		mt := "image/jpeg"
		switch {
		case strings.Contains(ct, "png"):
			mt = "image/png"
		case strings.Contains(ct, "gif"):
			mt = "image/gif"
		case strings.Contains(ct, "webp"):
			mt = "image/webp"
		}
		return imgBytes, mt, "", 0
	}

	filename := strings.TrimSpace(r.FormValue("filename"))
	if filename == "" {
		return nil, "", "provide 'photo' file, 'img_url', or 'filename' field", http.StatusBadRequest
	}
	filename = filepath.Base(filename)
	imgBytes, err = os.ReadFile(filepath.Join(photoDir, filename))
	if err != nil {
		return nil, "", "photo not found: " + filename, http.StatusNotFound
	}
	return imgBytes, mimeFromExt(filepath.Ext(filename)), "", 0
}

// parseLLMText strips markdown fences and parses the leaderboard JSON into a Section.
func parseLLMText(raw string) (*Section, string) {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)

	var extracted struct {
		SectionName string `json:"section_name"`
		Entries     []struct {
			Rank   int    `json:"rank"`
			Phone  string `json:"phone"`
			Amount int    `json:"amount"`
		} `json:"entries"`
	}
	if err := json.Unmarshal([]byte(raw), &extracted); err != nil {
		return nil, raw
	}
	entries := make([]SpenderEntry, len(extracted.Entries))
	for i, e := range extracted.Entries {
		entries[i] = SpenderEntry{Rank: e.Rank, Phone: e.Phone, Amount: e.Amount}
	}
	entriesJSON, _ := json.Marshal(entries)
	sec := &Section{Name: extracted.SectionName, Type: "spender", Entries: json.RawMessage(entriesJSON)}
	return sec, ""
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

	imgBytes, mediaType, errMsg, errStatus := readImageFromRequest(r)
	if errMsg != "" {
		writeError(w, errStatus, errMsg)
		return
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

	section, rawFallback := parseLLMText(claudeResp.Content[0].Text)
	if section == nil {
		writeJSON(w, http.StatusOK, map[string]string{"raw": rawFallback})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"section": section})
}

// POST /api/extract-gpt — same as /api/extract but uses OpenAI GPT-4o
func extractGPTHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		writeError(w, http.StatusInternalServerError, "OPENAI_API_KEY not set")
		return
	}

	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "failed to parse form")
		return
	}

	imgBytes, mediaType, errMsg, errStatus := readImageFromRequest(r)
	if errMsg != "" {
		writeError(w, errStatus, errMsg)
		return
	}

	b64 := base64.StdEncoding.EncodeToString(imgBytes)
	dataURL := fmt.Sprintf("data:%s;base64,%s", mediaType, b64)

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

	reqBody := gptRequest{
		Model:     "gpt-4o",
		MaxTokens: 2048,
		Messages: []gptMessage{
			{
				Role: "user",
				Content: []gptBlock{
					{Type: "image_url", ImageURL: &gptImageURL{URL: dataURL}},
					{Type: "text", Text: prompt},
				},
			},
		},
	}

	bodyBytes, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest(http.MethodPost, "https://api.openai.com/v1/chat/completions", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		writeError(w, http.StatusBadGateway, "openai api error: "+err.Error())
		return
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		var apiErr struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		json.Unmarshal(respBytes, &apiErr)
		msg := apiErr.Error.Message
		if msg == "" {
			msg = fmt.Sprintf("openai API status %d: %s", resp.StatusCode, string(respBytes))
		}
		writeError(w, http.StatusBadGateway, msg)
		return
	}

	var gptResp gptResponse
	if err := json.Unmarshal(respBytes, &gptResp); err != nil || len(gptResp.Choices) == 0 {
		writeError(w, http.StatusBadGateway, "failed to parse openai response: "+string(respBytes))
		return
	}

	section, rawFallback := parseLLMText(gptResp.Choices[0].Message.Content)
	if section == nil {
		writeJSON(w, http.StatusOK, map[string]string{"raw": rawFallback})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"section": section})
}

// callClaudeAutoSection sends one image to Claude and returns a Section with auto-detected type.
func callClaudeAutoSection(apiKey string, imgBytes []byte, mediaType string) (*Section, error) {
	b64 := base64.StdEncoding.EncodeToString(imgBytes)

	prompt := `This image shows a leaderboard from a fan-meeting event.
Extract all data and return ONLY a valid JSON object.

If it shows a TOP SPENDER / TOP STUDIO leaderboard (rows have Thai baht amounts):
{"section_name": "TOP SPENDER", "type": "spender", "entries": [{"rank": 1, "phone": "091-079xxxx", "amount": 77420}]}

If it shows a LUCKY FAN / LUCKY DRAW leaderboard (rows have names, no money):
{"section_name": "LUCKY FAN", "type": "lucky", "entries": [{"rank": 1, "name": "นาย ก***", "phone": "091-079xxxx"}]}

Rules:
- Detect type from content: spender has money/amount columns, lucky has name columns.
- Include every visible row sorted by rank ascending.
- "phone": copy the masked number exactly as shown.
- "amount": integer Thai baht, no commas or symbols (spender only).
- Return ONLY the JSON object. No markdown, no explanation.`

	reqBody := claudeRequest{
		Model:     "claude-sonnet-4-6",
		MaxTokens: 2048,
		Messages: []claudeMessage{
			{
				Role: "user",
				Content: []claudeBlock{
					{Type: "image", Source: &claudeSource{Type: "base64", MediaType: mediaType, Data: b64}},
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
		return nil, err
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("claude API status %d: %s", resp.StatusCode, string(respBytes))
	}

	var claudeResp claudeResponse
	if err := json.Unmarshal(respBytes, &claudeResp); err != nil || len(claudeResp.Content) == 0 {
		return nil, fmt.Errorf("failed to parse claude response")
	}

	raw := strings.TrimSpace(claudeResp.Content[0].Text)
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)

	var result struct {
		SectionName string          `json:"section_name"`
		Type        string          `json:"type"`
		Entries     json.RawMessage `json:"entries"`
	}
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return nil, fmt.Errorf("failed to parse extracted JSON: %s", raw)
	}

	return &Section{Name: result.SectionName, Type: result.Type, Entries: result.Entries}, nil
}

// POST /api/extract-multi — accepts photos[] files, auto-detects section type per image
func extractMultiHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		writeError(w, http.StatusInternalServerError, "ANTHROPIC_API_KEY not set")
		return
	}

	if err := r.ParseMultipartForm(128 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "failed to parse form")
		return
	}

	fileHeaders := r.MultipartForm.File["photos"]
	if len(fileHeaders) == 0 {
		writeError(w, http.StatusBadRequest, "provide at least one image in 'photos' field")
		return
	}

	type result struct {
		section *Section
		errMsg  string
		name    string
	}

	results := make([]result, len(fileHeaders))
	done := make(chan struct{}, len(fileHeaders))

	for i, fh := range fileHeaders {
		i, fh := i, fh
		go func() {
			defer func() { done <- struct{}{} }()
			f, err := fh.Open()
			if err != nil {
				results[i] = result{name: fh.Filename, errMsg: "open error: " + err.Error()}
				return
			}
			imgBytes, err := io.ReadAll(f)
			f.Close()
			if err != nil {
				results[i] = result{name: fh.Filename, errMsg: "read error: " + err.Error()}
				return
			}
			sec, err := callClaudeAutoSection(apiKey, imgBytes, mimeFromExt(filepath.Ext(fh.Filename)))
			if err != nil {
				results[i] = result{name: fh.Filename, errMsg: err.Error()}
				return
			}
			results[i] = result{name: fh.Filename, section: sec}
		}()
	}

	for range fileHeaders {
		<-done
	}

	var sections []Section
	var errors []string
	for _, res := range results {
		if res.section != nil {
			sections = append(sections, *res.section)
		} else if res.errMsg != "" {
			errors = append(errors, res.name+": "+res.errMsg)
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"sections": sections,
		"errors":   errors,
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
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file found, using system env")
		log.Println(err)
	}

	if err := loadDB(); err != nil {
		log.Fatalf("failed to load data.json: %v", err)
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/api/events", eventsHandler)
	mux.HandleFunc("/api/events/", eventByIDHandler)
	mux.HandleFunc("/api/stats", statsHandler)
	mux.HandleFunc("/api/extract", extractHandler)
	mux.HandleFunc("/api/extract-gpt", extractGPTHandler)
	mux.HandleFunc("/api/extract-multi", extractMultiHandler)

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
