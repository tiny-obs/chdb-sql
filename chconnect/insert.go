package chconnect

import "strings"

// insertStmt is "INSERT INTO [database.]table [(columns)] FORMAT <format>" and the data that follows it.
type insertStmt struct {
	database, table string
	columns         []string
	format          string
	data            string
}

// parseInsert recognises exactly the statement shape that can go through the Insert RPC and reports false for anything
// else (INSERT ... VALUES, INSERT ... SELECT, a FORMAT clause without data, a SETTINGS clause, comments, a second
// statement), which then runs through Exec unchanged. Keywords are case-insensitive. Identifiers are bare
// ([A-Za-z_][A-Za-z0-9_]*) or quoted with backticks or double quotes, with ClickHouse's escapes (a doubled quote, or a
// backslash before \ ' " ` n t r 0).
func parseInsert(sql string) (ins insertStmt, ok bool) {
	p := &scanner{s: sql}
	p.skipSpace()
	if !p.keyword("INSERT") || !p.space() || !p.keyword("INTO") || !p.space() {
		return ins, false
	}
	first, ok := p.ident()
	if !ok {
		return ins, false
	}
	ins.table = first
	if p.eat('.') {
		if ins.table, ok = p.ident(); !ok {
			return ins, false
		}
		ins.database = first
	}
	sawSpace := p.space()
	if p.eat('(') {
		for {
			p.skipSpace()
			col, ok := p.ident()
			if !ok {
				return ins, false
			}
			ins.columns = append(ins.columns, col)
			p.skipSpace()
			if p.eat(',') {
				continue
			}
			if p.eat(')') {
				break
			}
			return ins, false
		}
		sawSpace = true // FORMAT may follow the closing parenthesis at once
		p.skipSpace()
	}
	if !sawSpace || !p.keyword("FORMAT") || !p.space() {
		return ins, false
	}
	start := p.i
	for p.i < len(p.s) && isWord(p.s[p.i]) {
		p.i++
	}
	if p.i == start || p.i == len(p.s) || !isSpace(p.s[p.i]) {
		return ins, false // no format name, no data, or something other than whitespace after the name
	}
	ins.format = p.s[start:p.i]
	// ClickHouse: the data starts after the first newline, or after the blanks when there is none.
	for p.i < len(p.s) && (p.s[p.i] == ' ' || p.s[p.i] == '\t' || p.s[p.i] == '\f') {
		p.i++
	}
	if p.i < len(p.s) && p.s[p.i] == '\r' {
		p.i++
	}
	if p.i < len(p.s) && p.s[p.i] == '\n' {
		p.i++
	}
	ins.data = p.s[p.i:]
	if strings.TrimSpace(ins.data) == "" {
		return ins, false
	}
	return ins, true
}

type scanner struct {
	s string
	i int
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' }

func isWord(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

// space consumes whitespace and reports whether there was any.
func (p *scanner) space() bool {
	start := p.i
	p.skipSpace()
	return p.i > start
}

func (p *scanner) skipSpace() {
	for p.i < len(p.s) && isSpace(p.s[p.i]) {
		p.i++
	}
}

func (p *scanner) eat(c byte) bool {
	if p.i < len(p.s) && p.s[p.i] == c {
		p.i++
		return true
	}
	return false
}

// keyword consumes kw, ignoring case, when a whole word follows (the next byte is not a word byte).
func (p *scanner) keyword(kw string) bool {
	end := p.i + len(kw)
	if end > len(p.s) || !strings.EqualFold(p.s[p.i:end], kw) || (end < len(p.s) && isWord(p.s[end])) {
		return false
	}
	p.i = end
	return true
}

// ident consumes one bare or quoted identifier and returns its unquoted name. It rejects an empty name, an unknown
// escape and an unterminated quote.
func (p *scanner) ident() (string, bool) {
	if p.i >= len(p.s) {
		return "", false
	}
	q := p.s[p.i]
	if q != '`' && q != '"' {
		start := p.i
		if c := p.s[p.i]; c >= '0' && c <= '9' {
			return "", false
		}
		for p.i < len(p.s) && isWord(p.s[p.i]) {
			p.i++
		}
		return p.s[start:p.i], p.i > start
	}
	p.i++
	var sb strings.Builder
	for p.i < len(p.s) {
		c := p.s[p.i]
		p.i++
		switch {
		case c == q:
			if p.i < len(p.s) && p.s[p.i] == q { // doubled quote
				sb.WriteByte(q)
				p.i++
				continue
			}
			return sb.String(), sb.Len() > 0
		case c == '\\':
			if p.i >= len(p.s) {
				return "", false
			}
			e := p.s[p.i]
			p.i++
			switch e {
			case '\\', '\'', '"', '`':
				sb.WriteByte(e)
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			case 'r':
				sb.WriteByte('\r')
			case '0':
				sb.WriteByte(0)
			default:
				return "", false
			}
		default:
			sb.WriteByte(c)
		}
	}
	return "", false
}
