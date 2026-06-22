// Entity-based cross-venue market matcher for Kalshi ↔ Polymarket
// Decomposes MVE legs and structured questions, then matches by normalized entity overlap.

const TEAM_ALIASES = {
  // NBA
  'philadelphia': ['philadelphia 76ers', '76ers', 'philly'],
  'los angeles l': ['los angeles lakers', 'lakers'],
  'la lakers': ['los angeles lakers', 'lakers'],
  'lakers': ['los angeles lakers'],
  'la clippers': ['los angeles clippers', 'clippers'],
  'clippers': ['los angeles clippers'],
  'golden state': ['golden state warriors', 'warriors'],
  'warriors': ['golden state warriors'],
  'boston': ['boston celtics', 'celtics'],
  'celtics': ['boston celtics'],
  'milwaukee': ['milwaukee bucks', 'bucks'],
  'bucks': ['milwaukee bucks'],
  'brooklyn': ['brooklyn nets', 'nets'],
  'nets': ['brooklyn nets'],
  'miami': ['miami heat', 'heat'],
  'new york': ['new york knicks', 'knicks'],
  'knicks': ['new york knicks'],
  'chicago': ['chicago bulls', 'chicago cubs', 'chicago blackhawks', 'chicago bears'],
  'bulls': ['chicago bulls'],
  'cavaliers': ['cleveland cavaliers', 'cleveland'],
  'cleveland': ['cleveland cavaliers'],
  'detroit': ['detroit pistons', 'detroit red wings', 'detroit lions', 'detroit tigers'],
  'pistons': ['detroit pistons'],
  'indiana': ['indiana pacers', 'pacers'],
  'pacers': ['indiana pacers'],
  'toronto': ['toronto raptors', 'raptors', 'toronto blue jays', 'toronto maple leafs'],
  'raptors': ['toronto raptors'],
  'atlanta': ['atlanta hawks', 'hawks'],
  'hawks': ['atlanta hawks'],
  'charlotte': ['charlotte hornets', 'hornets'],
  'hornets': ['charlotte hornets'],
  'orlando': ['orlando magic', 'magic'],
  'magic': ['orlando magic'],
  'washington': ['washington wizards', 'washington commanders', 'washington capitals', 'washington nationals'],
  'wizards': ['washington wizards'],
  'dallas': ['dallas mavericks', 'dallas cowboys', 'dallas stars', 'mavericks'],
  'mavericks': ['dallas mavericks'],
  'houston': ['houston rockets', 'rockets'],
  'rockets': ['houston rockets'],
  'memphis': ['memphis grizzlies', 'grizzlies'],
  'grizzlies': ['memphis grizzlies'],
  'new orleans': ['new orleans pelicans', 'pelicans'],
  'pelicans': ['new orleans pelicans'],
  'oklahoma city': ['oklahoma city thunder', 'oklahoma', 'thunder'],
  'thunder': ['oklahoma city thunder'],
  'san antonio': ['san antonio spurs', 'spurs'],
  'spurs': ['san antonio spurs'],
  'utah': ['utah jazz', 'jazz'],
  'portland': ['portland trail blazers', 'trail blazers', 'blazers'],
  'trail blazers': ['portland trail blazers'],
  'denver': ['denver nuggets', 'nuggets'],
  'nuggets': ['denver nuggets'],
  'minnesota': ['minnesota timberwolves', 'timberwolves', 'minnesota wild', 'minnesota vikings', 'minnesota twins'],
  'timberwolves': ['minnesota timberwolves'],
  'sacramento': ['sacramento kings', 'kings'],
  'kings': ['sacramento kings'],
  'phoenix': ['phoenix suns', 'suns'],
  'suns': ['phoenix suns'],
  // MLB
  'yankees': ['new york yankees'],
  'mets': ['new york mets'],
  'red sox': ['boston red sox'],
  'dodgers': ['los angeles dodgers'],
  'los angeles a': ['los angeles angels', 'anaheim angels'],
  'los angeles d': ['los angeles dodgers', 'la dodgers'],
  'angels': ['los angeles angels'],
  'cubs': ['chicago cubs'],
  'chicago c': ['chicago cubs'],
  'chicago w': ['chicago white sox'],
  'white sox': ['chicago white sox'],
  'astros': ['houston astros'],
  'braves': ['atlanta braves'],
  'phillies': ['philadelphia phillies'],
  'cardinals': ['st. louis cardinals', 'st louis cardinals'],
  'st. louis': ['st. louis cardinals', 'st. louis blues'],
  'giants': ['san francisco giants', 'new york giants'],
  'padres': ['san diego padres'],
  'mariners': ['seattle mariners'],
  'seattle': ['seattle mariners', 'seattle kraken', 'seattle seahawks', 'seattle sounders'],
  'guardians': ['cleveland guardians'],
  'brewers': ['milwaukee brewers'],
  'pirates': ['pittsburgh pirates'],
  'pittsburgh': ['pittsburgh pirates', 'pittsburgh steelers', 'pittsburgh penguins'],
  'reds': ['cincinnati reds'],
  'cincinnati': ['cincinnati reds', 'cincinnati bengals'],
  'royals': ['kansas city royals'],
  'athletics': ['oakland athletics', 'oakland a\'s', 'a\'s'],
  'rays': ['tampa bay rays', 'tampa bay'],
  'tampa bay': ['tampa bay rays', 'tampa bay lightning', 'tampa bay buccaneers'],
  'orioles': ['baltimore orioles'],
  'blue jays': ['toronto blue jays'],
  'rockies': ['colorado rockies'],
  'colorado': ['colorado rockies', 'colorado avalanche'],
  'diamondbacks': ['arizona diamondbacks'],
  'arizona': ['arizona diamondbacks', 'arizona coyotes', 'arizona cardinals'],
  'marlins': ['miami marlins'],
  'rangers': ['texas rangers'],
  'texas': ['texas rangers', 'dallas cowboys'],
  'twins': ['minnesota twins'],
  'tigers': ['detroit tigers'],
  // NHL
  'bruins': ['boston bruins'],
  'penguins': ['pittsburgh penguins'],
  'flyers': ['philadelphia flyers'],
  'canadiens': ['montreal canadiens'],
  'maple leafs': ['toronto maple leafs'],
  'lightning': ['tampa bay lightning'],
  'hurricanes': ['carolina hurricanes'],
  'predators': ['nashville predators'],
  'blackhawks': ['chicago blackhawks'],
  'blues': ['st. louis blues'],
  'avalanche': ['colorado avalanche'],
  'golden knights': ['vegas golden knights'],
  'vegas': ['vegas golden knights'],
  'oilers': ['edmonton oilers'],
  'flames': ['calgary flames'],
  'canucks': ['vancouver canucks'],
  'jets': ['winnipeg jets'],
  'sabres': ['buffalo sabres'],
  'senators': ['ottawa senators'],
  'sharks': ['san jose sharks'],
  'ducks': ['anaheim ducks'],
  'kraken': ['seattle kraken'],
  'coyotes': ['arizona coyotes'],
  'panthers': ['florida panthers'],
  'devils': ['new jersey devils'],
  'islanders': ['new york islanders'],
  'rangers': ['new york rangers'],
  'wild': ['minnesota wild'],
  'blue jackets': ['columbus blue jackets'],
  'stars': ['dallas stars'],
  // NFL
  'eagles': ['philadelphia eagles'],
  'cowboys': ['dallas cowboys'],
  'chiefs': ['kansas city chiefs'],
  'bills': ['buffalo bills'],
  'bengals': ['cincinnati bengals'],
  '49ers': ['san francisco 49ers'],
  'ravens': ['baltimore ravens'],
  'chargers': ['los angeles chargers'],
  'vikings': ['minnesota vikings'],
  'dolphins': ['miami dolphins'],
  'patriots': ['new england patriots'],
  'packers': ['green bay packers'],
  'bears': ['chicago bears'],
  'colts': ['indianapolis colts'],
  'broncos': ['denver broncos'],
  'steelers': ['pittsburgh steelers'],
  'raiders': ['las vegas raiders'],
  'browns': ['cleveland browns'],
  'jaguars': ['jacksonville jaguars'],
  'saints': ['new orleans saints'],
  'buccaneers': ['tampa bay buccaneers'],
  'cardinals': ['arizona cardinals'],
  'titans': ['tennessee titans'],
  'commanders': ['washington commanders'],
  // Soccer / World
  'argentina': ['argentina'],
  'france': ['france'],
  'portugal': ['portugal'],
  'brazil': ['brazil'],
  'england': ['england'],
  'netherlands': ['netherlands', 'holland'],
  'germany': ['germany'],
  'spain': ['spain'],
  'italy': ['italy'],
  'croatia': ['croatia'],
  'morocco': ['morocco'],
  'belgium': ['belgium'],
  'uruguay': ['uruguay'],
  'switzerland': ['switzerland'],
  'japan': ['japan'],
  'australia': ['australia'],
  'usa': ['usa', 'united states', 'america'],
  'united states': ['usa', 'united states', 'america'],
  'new zealand': ['new zealand'],
  'nigeria': ['nigeria'],
  'ghana': ['ghana'],
  'ivory coast': ['ivory coast', 'côte d\'ivoire'],
  'senegal': ['senegal'],
  'cameroon': ['cameroon'],
  'tunisia': ['tunisia'],
  'algeria': ['algeria'],
  'egypt': ['egypt'],
  'south korea': ['south korea'],
  'iran': ['iran'],
  'iraq': ['iraq'],
  'austria': ['austria'],
  'norway': ['norway'],
  'sweden': ['sweden'],
  'denmark': ['denmark'],
  'poland': ['poland'],
  'ukraine': ['ukraine'],
  'turkey': ['turkey'],
  'russia': ['russia'],
  'manchester city': ['manchester city', 'man city'],
  'manchester united': ['manchester united', 'man utd'],
  'liverpool': ['liverpool'],
  'chelsea': ['chelsea'],
  'arsenal': ['arsenal'],
  'tottenham': ['tottenham hotspur'],
  'newcastle': ['newcastle united'],
  'real madrid': ['real madrid'],
  'barcelona': ['barcelona'],
  'bayern munich': ['bayern munich', 'bayern'],
  'psg': ['paris saint-germain', 'paris'],
  'inter milan': ['inter milan', 'inter'],
  'ac milan': ['ac milan'],
  'juventus': ['juventus'],
  // Politics
  'biden': ['joe biden'],
  'trump': ['donald trump'],
  'desantis': ['ron desantis'],
  'harris': ['kamala harris'],
  'newsom': ['gavin newsom'],
  'haley': ['nikki haley'],
  'scott': ['tim scott'],
};

const STOP_WORDS = new Set([
  'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'can', 'could', 'did', 'do', 'does', 'for', 'from', 'has', 'have', 'how', 'in', 'is', 'it', 'its', 'may', 'might', 'more', 'of', 'on', 'or', 'shall', 'should', 'than', 'that', 'the', 'their', 'there', 'these', 'this', 'those', 'to', 'was', 'were', 'what', 'when', 'where', 'which', 'who', 'whom', 'will', 'with', 'would', 'yes', 'no', 'not', 'over', 'under', 'above', 'below', 'within', 'between', 'after', 'before', 'during', 'year', 'years', 'month', 'months', 'day', 'days', 'week', 'weeks', 'case', 'cases', 'market', 'markets', 'event', 'events', 'question', 'questions', 'win', 'wins', 'team', 'teams', 'player', 'players', 'election', 'elections', 'price', 'prices'
]);

function normalizeName(name) {
  const key = name.toLowerCase().replace(/^yes\s+/i, '').replace(/^no\s+/i, '').trim();
  const alias = TEAM_ALIASES[key] || TEAM_ALIASES[key.replace(/s$/, '')];
  if (alias) return alias;
  // Strip player stats like "Mike Trout: 2+" → "mike trout"
  const clean = key.replace(/:.*$/, '').replace(/\s+\d+\+?$/, '').trim();
  if (clean !== key) {
    const alias2 = TEAM_ALIASES[clean] || TEAM_ALIASES[clean.replace(/s$/, '')];
    if (alias2) return alias2;
  }
  return [clean];
}

function extractKeywords(text) {
  const tokens = String(text || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]+/g, ' ')
    .split(/\s+/)
    .map(t => t.trim())
    .filter(t => t.length > 2 && !STOP_WORDS.has(t) && !/^\d+$/.test(t));
  return [...new Set(tokens)];
}

// === Kalshi leg parsing ===
function parseKalshiLegs(market) {
  const title = market.title || '';
  if (!title) return [];
  const rawLegs = title.split(',').map(s => s.trim()).filter(Boolean);
  return rawLegs.map(leg => {
    const side = leg.startsWith('no ') ? 'no' : 'yes';
    const entity = leg.replace(/^(yes|no)\s+/i, '').trim();
    const isStatLine = /:\s*\d+\+?/.test(entity) || /wins by over/i.test(entity) || /over \d+\.?\d* (runs|goals|points)/i.test(entity);
    const isTeamLine = !isStatLine;
    const names = normalizeName(entity);
    return { side, entity, names, keywords: extractKeywords(entity), isStatLine, isTeamLine };
  });
}

// === Polymarket question parsing ===
function parsePolymarketQuestion(market) {
  const q = market.question || market.title || '';
  if (!q) return { raw: q, league: null, teams: [], date: null, type: 'unknown' };

  // Pattern 1: "NBA: LA Clippers vs. Orlando Magic 2023-03-18"
  let m = q.match(/^(\w+):\s*(.+?)\s+vs\.?\s+(.+?)(?:\s+(\d{4}[\-\/]\d{1,2}[\-\/]\d{1,2}))?\s*$/i);
  if (m) {
    const teamNames = [...normalizeName(m[2].trim()), ...normalizeName(m[3].trim())];
    return { raw: q, league: m[1].toUpperCase(), teams: teamNames.flat(), keywords: extractKeywords(q), date: m[4] || null, type: 'matchup' };
  }

  // Pattern 2: "Will Chelsea beat Dortmund? (03/07/2023)"
  m = q.match(/^Will\s+(.+?)\s+beat\s+(.+?)\??(?:\s*\((\d{1,2}\/\d{1,2}\/\d{4})\))?\s*$/i);
  if (m) {
    const teamNames = [...normalizeName(m[1].trim()), ...normalizeName(m[2].trim().replace(/[\?\.]$/, ''))];
    return { raw: q, league: 'SOCCER', teams: teamNames.flat(), keywords: extractKeywords(q), date: m[3] || null, type: 'matchup' };
  }

  // Pattern 3: "MLB: Who will win Team A v. Team B, scheduled for date"
  m = q.match(/^(\w+):\s*Who will win\s+(.+?)\s+v[.]?\s+(.+?)(?:,|\s+scheduled)/i);
  if (m) {
    const teamNames = [...normalizeName(m[2].trim()), ...normalizeName(m[3].trim())];
    return { raw: q, league: m[1].toUpperCase(), teams: teamNames.flat(), keywords: extractKeywords(q), date: null, type: 'matchup' };
  }

  // Pattern 4: "NFL Sunday: Team A vs. Team B"
  m = q.match(/^(NFL|NBA|NHL|MLB|NCAAB)\s+\w+:\s*(.+?)\s+vs\.?\s+(.+?)$/i);
  if (m) {
    const teamNames = [...normalizeName(m[2].trim()), ...normalizeName(m[3].trim())];
    return { raw: q, league: m[1].toUpperCase(), teams: teamNames.flat(), keywords: extractKeywords(q), date: null, type: 'matchup' };
  }

  // Pattern 5: "World Cup: Day - Team A vs. Team B"
  m = q.match(/^World Cup/i);
  if (m) {
    const parts = q.split(/\s+vs\.?\s+/i);
    if (parts.length >= 2) {
      const teamA = parts[0].replace(/^World Cup.*?-\s*/i, '').trim();
      const teamB = parts[1].trim();
      const teamNames = [...normalizeName(teamA), ...normalizeName(teamB)];
      return { raw: q, league: 'WORLDCUP', teams: teamNames.flat(), keywords: extractKeywords(q), date: null, type: 'matchup' };
    }
  }

  // Pattern 6: "Will {Person/Team} win {Event/Award}?"
  m = q.match(/^Will\s+(.+?)\s+win\s+(.+?)\??$/i);
  if (m) {
    const names = normalizeName(m[1].trim());
    const subject = m[1].trim();
    const award = m[2].trim();
    return { raw: q, league: null, teams: names, keywords: extractKeywords(q), date: null, type: 'win', subject, award };
  }

  // Pattern 7: "[Single Market] Will {Person} win {event}?"
  m = q.match(/^\[Single Market\]\s+Will\s+(.+?)\s+win\s+(.+?)\??$/i);
  if (m) {
    const names = normalizeName(m[1].trim());
    return { raw: q, league: 'POLITICS', teams: names, keywords: extractKeywords(q), date: null, type: 'election', subject: m[1].trim(), award: m[2].trim() };
  }

  // Pattern 8: "{League}: Who will win - {Team A} or {Team B}?"
  m = q.match(/^(\w+):\s*Who will win\s*-\s*(.+?)\s+or\s+(.+?)\??$/i);
  if (m) {
    const teamNames = [...normalizeName(m[2].trim()), ...normalizeName(m[3].trim())];
    return { raw: q, league: m[1].toUpperCase(), teams: teamNames.flat(), keywords: extractKeywords(q), date: null, type: 'matchup' };
  }

  // Pattern 9: "UFC {number}: Who will win - {A} or {B}?"
  m = q.match(/^UFC\s+\d+.*?:\s*Who will win\s*-\s*(.+?)\s+or\s+(.+?)\??$/i);
  if (m) {
    const names = [...normalizeName(m[1].trim()), ...normalizeName(m[2].trim())];
    return { raw: q, league: 'UFC', teams: names.flat(), keywords: extractKeywords(q), date: null, type: 'matchup' };
  }

  // Pattern 10: "Will {Person/Entity} {do something}?"
  m = q.match(/^Will\s+(.+?)\s+(\w.+?)\??$/i);
  if (m) {
    const names = normalizeName(m[1].trim().replace(/[\?\.]$/, ''));
    return { raw: q, league: null, teams: names, keywords: extractKeywords(q), date: null, type: 'binary', subject: m[1].trim(), predicate: m[2].trim() };
  }

  // Fallback: extract any team/entity names via normalization
  const tokens = q.split(/[,\s]+/).filter(t => t.length > 2);
  const names = tokens.flatMap(t => normalizeName(t));
  return { raw: q, league: null, teams: [...new Set(names.flat())], keywords: extractKeywords(q), date: null, type: 'unknown' };
}

function keywordOverlap(aKeywords = [], bKeywords = []) {
  const a = [...new Set(aKeywords.filter(Boolean).map(k => k.toLowerCase()))];
  const b = [...new Set(bKeywords.filter(Boolean).map(k => k.toLowerCase()))];
  if (!a.length || !b.length) return { score: 0, hits: [] };
  const setB = new Set(b);
  const hits = a.filter(k => setB.has(k));
  const union = new Set([...a, ...b]);
  return { score: hits.length / Math.max(1, union.size), hits };
}

function wordBoundaryMatch(longer, shorter) {
  const esc = shorter.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp('\\b' + esc + '\\b', 'i').test(longer);
}

function entityOverlap(kalshiNames, polymarketTeams) {
  if (!kalshiNames.length || !polymarketTeams.length) return { score: 0, hits: [] };
  const pTeams = [...new Set(polymarketTeams.filter(t => typeof t === 'string').map(t => t.toLowerCase()))];
  const hits = [];
  for (const kn of kalshiNames) {
    if (typeof kn !== 'string') continue;
    const knl = kn.toLowerCase();
    for (const ptl of pTeams) {
      if (knl === ptl) { hits.push(kn); continue; }
      const shorter = knl.length <= ptl.length ? knl : ptl;
      const longer = knl.length > ptl.length ? knl : ptl;
      if (wordBoundaryMatch(longer, shorter)) hits.push(kn);
    }
  }
  const uniqueHits = [...new Set(hits)];
  const allKNames = kalshiNames.filter(n => typeof n === 'string').map(n => n.toLowerCase());
  const totalUnique = new Set([...allKNames, ...pTeams]);
  const score = uniqueHits.length / Math.max(1, totalUnique.size);
  return { score: Math.min(1, score * 2), hits: uniqueHits };
}

export function matchMarkets(kalshiMarkets, polymarketMarkets, options = {}) {
  const minScore = options.minScore || 0.05;
  const results = [];

  // Pre-parse Polymarket questions
  const parsedPolymarkets = polymarketMarkets.map(p => ({
    market: p,
    parsed: parsePolymarketQuestion(p),
  }));

  for (const k of kalshiMarkets) {
    const kLegs = parseKalshiLegs(k);
    if (!kLegs.length) continue;
    const kYesLegs = kLegs.filter(l => l.side === 'yes' && l.isTeamLine);
    const kAllNames = [...new Set(kLegs.flatMap(l => l.names))];

    for (const { market: p, parsed: pp } of parsedPolymarkets) {
      // Decomposed matching: compare individual Kalshi legs vs Polymarket entities
      let bestScore = 0;
      let bestHits = [];
      let matchLeg = null;

      for (const leg of kYesLegs) {
        const { score, hits } = entityOverlap(leg.names, pp.teams);
        if (score > bestScore) {
          bestScore = score;
          bestHits = hits;
          matchLeg = leg;
        }
      }

      // If no specific leg matches, try all names
      if (bestScore < minScore) {
        const { score, hits } = entityOverlap(kAllNames, pp.teams);
        if (score > bestScore) {
          bestScore = score;
          bestHits = hits;
        }
      }

      const kalshiKeywords = kLegs.flatMap(l => l.keywords || []);
      const polyKeywords = pp.keywords || [];
      const keywordScore = keywordOverlap(kalshiKeywords, polyKeywords);
      if (keywordScore.score > bestScore) {
        bestScore = keywordScore.score;
        bestHits = keywordScore.hits;
      }

      if (bestScore >= minScore) {
        results.push({
          kalshiMarket: k,
          polymarketMarket: p,
          similarity: {
            overall: Number(bestScore.toFixed(4)),
            entityOverlapScore: Number(bestScore.toFixed(4)),
            matchedEntities: bestHits,
            kalshiLeg: matchLeg || null,
            polymarketType: pp.type,
          },
        });
      }
    }
  }

  results.sort((a, b) => b.similarity.overall - a.similarity.overall);

  // Deduplicate: keep best match per unique Kalshi+Polymarket pair
  const best = new Map();
  for (const r of results) {
    const key = `${r.kalshiMarket.id || r.kalshiMarket.ticker}->${r.polymarketMarket.condition_id || r.polymarketMarket.id || r.polymarketMarket.question}`;
    if (!best.has(key) || r.similarity.overall > best.get(key).similarity.overall) {
      best.set(key, r);
    }
  }

  return Array.from(best.values()).slice(0, options.maxMatches || 250);
}
