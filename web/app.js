const snapshotForm = document.getElementById("snapshot-form")
const snapshotType = document.getElementById("snapshot-type")
const snapshotDate = document.getElementById("snapshot-date")
const snapshotLanguage = document.getElementById("snapshot-language")
const snapshotMeta = document.getElementById("snapshot-meta")

const searchForm = document.getElementById("search-form")
const searchType = document.getElementById("search-type")
const searchQuery = document.getElementById("search-query")
const searchStart = document.getElementById("search-start")
const searchEnd = document.getElementById("search-end")
const searchLanguage = document.getElementById("search-language")
const searchMeta = document.getElementById("search-meta")

const resultsBody = document.getElementById("results-body")
const resultsCount = document.getElementById("results-count")
const lastUpdated = document.getElementById("last-updated")

const basePath = window.location.pathname.replace(/[^/]*$/, "")
const apiBase = `${basePath}api`
const archiveBase = `${basePath}archive`

const MAX_SEARCH_DAYS = 370
const MS_PER_DAY = 24 * 60 * 60 * 1000

let apiMode = "server"
let staticManifest = null

function setMeta(el, message, isError = false) {
  el.textContent = message
  el.classList.toggle("error", isError)
}

async function fetchJson(url) {
  const res = await fetch(url)
  if (!res.ok) {
    const payload = await res.json().catch(() => ({}))
    throw new Error(payload.error || `Request failed: ${res.status}`)
  }
  return res.json()
}

function normalizeLanguageParam(value) {
  if (!value || value === "all") {
    return "(null)"
  }
  return value
}

function buildArchiveUrl(type, date, language) {
  const year = date.slice(0, 4)
  const encoded = encodeURIComponent(language)
  return `${archiveBase}/${type}/${year}/${date}/${encoded}.json`
}

function formatLanguageLabel(value) {
  if (value === "(null)") {
    return "All languages"
  }
  return value
}

function setResults(rows) {
  resultsCount.textContent = `${rows.length} rows`
  resultsBody.innerHTML = ""

  if (rows.length === 0) {
    const empty = document.createElement("div")
    empty.className = "empty"
    empty.textContent = "No results found."
    resultsBody.appendChild(empty)
    return
  }

  rows.forEach((row) => {
    const wrapper = document.createElement("div")
    wrapper.className = "result-row"

    const date = document.createElement("div")
    date.textContent = row.date

    const rank = document.createElement("div")
    rank.textContent = row.rank ? `#${row.rank}` : "-"

    const name = document.createElement("div")
    name.innerHTML = `<strong>${row.name}</strong> <span>${row.language ? row.language : "all"}</span>`

    wrapper.appendChild(date)
    wrapper.appendChild(rank)
    wrapper.appendChild(name)

    resultsBody.appendChild(wrapper)
  })
}

function setLastUpdated(value) {
  if (!lastUpdated) {
    return
  }

  if (!value) {
    lastUpdated.textContent = "Last updated: unavailable"
    return
  }

  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) {
    lastUpdated.textContent = "Last updated: unavailable"
    return
  }

  lastUpdated.textContent = `Last updated: ${parsed.toISOString()}`
}

async function resolveApiMode() {
  try {
    staticManifest = await fetchJson(`${apiBase}/manifest.json`)
    apiMode = "static"
  } catch (error) {
    apiMode = "server"
  }
}

async function apiLatest(type) {
  if (apiMode === "static") {
    const latestDate = staticManifest?.[type]?.latestDate ?? null
    return {date: latestDate}
  }
  return fetchJson(`${apiBase}/latest?type=${type}`)
}

async function apiLanguages(type, date) {
  if (apiMode === "static") {
    const languages = staticManifest?.[type]?.languagesByDate?.[date]
    if (!languages) {
      throw new Error(`No languages found for ${date}.`)
    }
    return {date, languages}
  }
  return fetchJson(`${apiBase}/languages?type=${type}&date=${date}`)
}

async function apiSnapshot(type, date, language) {
  const normalizedLanguage = normalizeLanguageParam(language)
  if (apiMode === "static") {
    return fetchJson(buildArchiveUrl(type, date, normalizedLanguage))
  }
  return fetchJson(
    `${apiBase}/snapshot?type=${type}&date=${date}&language=${encodeURIComponent(normalizedLanguage)}`
  )
}

async function apiSearch({type, query, start, end, language, limit}) {
  const normalizedLanguage = normalizeLanguageParam(language)
  const safeLimit = Number.isFinite(limit) && limit > 0 ? Math.min(limit, 500) : 200

  if (apiMode === "static") {
    return searchStatic({
      type,
      query,
      start,
      end,
      language: normalizedLanguage,
      limit: safeLimit,
    })
  }

  const params = new URLSearchParams({
    type,
    query,
    start,
    end,
    language: normalizedLanguage,
    limit: String(safeLimit),
  })
  return fetchJson(`${apiBase}/search?${params.toString()}`)
}

async function loadLastUpdated() {
  try {
    if (apiMode === "static") {
      const data = await fetchJson(`${apiBase}/last-updated.json`)
      setLastUpdated(data.updatedAt)
      return
    }

    const data = await fetchJson(`${apiBase}/last-updated`)
    setLastUpdated(data.updatedAt)
  } catch (error) {
    setLastUpdated(null)
  }
}

async function searchStatic({type, query, start, end, language, limit}) {
  if (!isIsoDate(start) || !isIsoDate(end)) {
    throw new Error("start/end must be YYYY-MM-DD")
  }

  const startDate = parseIsoDate(start)
  const endDate = parseIsoDate(end)
  if (!startDate || !endDate || startDate.getTime() > endDate.getTime()) {
    throw new Error("invalid date range")
  }

  const rangeDays = Math.floor((endDate.getTime() - startDate.getTime()) / MS_PER_DAY) + 1
  if (rangeDays > MAX_SEARCH_DAYS) {
    throw new Error(`date range too large (max ${MAX_SEARCH_DAYS} days)`)
  }

  const normalizedQuery = query.trim().toLowerCase()
  const results = []

  for (const dateValue of enumerateDates(startDate, endDate)) {
    let payload
    try {
      payload = await fetchJson(buildArchiveUrl(type, dateValue, language))
    } catch (error) {
      continue
    }

    if (!payload || !Array.isArray(payload.list)) {
      continue
    }

    for (let index = 0; index < payload.list.length; index += 1) {
      const entry = payload.list[index]
      if (typeof entry !== "string") {
        continue
      }

      if (!entry.toLowerCase().includes(normalizedQuery)) {
        continue
      }

      results.push({
        date: dateValue,
        language: payload.language ?? null,
        rank: index + 1,
        name: entry,
      })

      if (results.length >= limit) {
        return {count: results.length, results}
      }
    }
  }

  return {count: results.length, results}
}

function isIsoDate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(value)
}

function parseIsoDate(value) {
  if (!isIsoDate(value)) {
    return null
  }

  const [year, month, day] = value.split("-").map((part) => Number.parseInt(part, 10))
  if (!year || !month || !day) {
    return null
  }

  const date = new Date(Date.UTC(year, month - 1, day))
  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) {
    return null
  }

  return date
}

function formatIsoDate(date) {
  return [date.getUTCFullYear(), date.getUTCMonth() + 1, date.getUTCDate()]
    .map((value) => String(value).padStart(2, "0"))
    .join("-")
}

function enumerateDates(startDate, endDate) {
  const dates = []
  const cursor = new Date(startDate.getTime())
  while (cursor.getTime() <= endDate.getTime()) {
    dates.push(formatIsoDate(cursor))
    cursor.setUTCDate(cursor.getUTCDate() + 1)
  }
  return dates
}

async function populateLatestDates() {
  const [repoLatest, devLatest] = await Promise.all([
    apiLatest("repository"),
    apiLatest("developer"),
  ])

  if (repoLatest.date) {
    snapshotDate.value = repoLatest.date
    searchEnd.value = repoLatest.date
  }

  if (repoLatest.date && !searchStart.value) {
    const end = new Date(repoLatest.date)
    const start = new Date(end.getTime() - 6 * MS_PER_DAY)
    searchStart.value = start.toISOString().slice(0, 10)
  }

  if (snapshotType.value === "developer" && devLatest.date) {
    snapshotDate.value = devLatest.date
  }
}

async function populateLanguages() {
  if (!snapshotDate.value) {
    return
  }

  try {
    const data = await apiLanguages(snapshotType.value, snapshotDate.value)
    snapshotLanguage.innerHTML = ""

    data.languages.forEach((lang) => {
      const option = document.createElement("option")
      option.value = lang
      option.textContent = formatLanguageLabel(lang)
      snapshotLanguage.appendChild(option)
    })

    if (!data.languages.includes("(null)")) {
      const option = document.createElement("option")
      option.value = "(null)"
      option.textContent = "All languages"
      snapshotLanguage.appendChild(option)
    }

    snapshotLanguage.value = "(null)"
  } catch (error) {
    setMeta(snapshotMeta, error.message, true)
  }
}

snapshotType.addEventListener("change", async () => {
  await populateLatestDates()
  await populateLanguages()
})

snapshotDate.addEventListener("change", async () => {
  await populateLanguages()
})

snapshotForm.addEventListener("submit", async (event) => {
  event.preventDefault()
  setMeta(snapshotMeta, "Loading snapshot...")
  try {
    const data = await apiSnapshot(
      snapshotType.value,
      snapshotDate.value,
      snapshotLanguage.value
    )

    const rows = data.list.map((name, idx) => ({
      date: data.date,
      language: data.language,
      rank: idx + 1,
      name,
    }))

    setResults(rows)
    setMeta(snapshotMeta, `${rows.length} entries loaded for ${snapshotDate.value}.`)
  } catch (error) {
    setMeta(snapshotMeta, error.message, true)
  }
})

searchForm.addEventListener("submit", async (event) => {
  event.preventDefault()
  setMeta(searchMeta, "Searching...")

  const language = searchLanguage.value.trim() || "(null)"

  try {
    const data = await apiSearch({
      type: searchType.value,
      query: searchQuery.value,
      start: searchStart.value,
      end: searchEnd.value,
      language,
      limit: 200,
    })

    setResults(data.results)
    setMeta(searchMeta, `${data.count} matches.`)
  } catch (error) {
    setMeta(searchMeta, error.message, true)
  }
})

async function init() {
  await resolveApiMode()
  await loadLastUpdated()
  await populateLatestDates()
  await populateLanguages()
  searchLanguage.value = "(null)"
}

init().catch((error) => {
  setMeta(snapshotMeta, error.message, true)
})
