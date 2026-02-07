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
const rangeButtons = Array.from(document.querySelectorAll("[data-range]"))

const viewLinks = Array.from(document.querySelectorAll("[data-view-link]"))
const viewSections = Array.from(document.querySelectorAll("[data-view]"))
const resultsSection = document.getElementById("results")
const resultsTitle = document.getElementById("results-title")

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

function getTodayIso() {
  return new Date().toISOString().slice(0, 10)
}

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
    empty.className =
      "rounded-xl border border-dashed border-neutral-200 px-4 py-8 text-center text-neutral-500"
    empty.textContent = "No results found."
    resultsBody.appendChild(empty)
    return
  }

  rows.forEach((row) => {
    const wrapper = document.createElement("div")
    wrapper.className =
      "grid grid-cols-[90px_70px_1fr] items-center gap-3 rounded-xl border border-neutral-200 bg-white px-4 py-3"

    const date = document.createElement("div")
    date.className = "text-neutral-500"
    date.textContent = row.date

    const rank = document.createElement("div")
    rank.className = "text-neutral-500"
    rank.textContent = row.rank ? `#${row.rank}` : "-"

    const name = document.createElement("div")
    const languageLabel = row.language ? row.language : "all"
    name.innerHTML = `<strong>${row.name}</strong> <span class="text-neutral-500">${languageLabel}</span>`

    wrapper.appendChild(date)
    wrapper.appendChild(rank)
    wrapper.appendChild(name)

    resultsBody.appendChild(wrapper)
  })
}

function normalizeView(value) {
  if (value === "search" || value === "range") {
    return "range"
  }
  return "snapshot"
}

function getViewFromLocation() {
  const path = window.location.pathname.replace(/\/+$/, "")
  if (path.endsWith("/search")) {
    return "range"
  }
  if (path.endsWith("/snapshot")) {
    return "snapshot"
  }
  const params = new URLSearchParams(window.location.search)
  const viewParam = params.get("view")
  if (viewParam) {
    return normalizeView(viewParam)
  }
  return "snapshot"
}

function buildViewPath(view) {
  const rootPath = basePath.endsWith("/") ? basePath.slice(0, -1) : basePath
  const suffix = view === "range" ? "/search" : "/snapshot"
  if (!rootPath) {
    return suffix
  }
  return `${rootPath}${suffix}`
}

function formatRelativeAge(value) {
  const deltaMs = Date.now() - value.getTime()
  if (!Number.isFinite(deltaMs)) {
    return null
  }

  const totalSeconds = Math.max(0, Math.floor(deltaMs / 1000))
  if (totalSeconds <= 60) {
    return `${totalSeconds}s ago`
  }

  const totalMinutes = Math.floor(totalSeconds / 60)
  if (totalMinutes < 60) {
    return `${totalMinutes}m ago`
  }

  const totalHours = Math.floor(totalMinutes / 60)
  if (totalHours < 24) {
    return `${totalHours}h ago`
  }

  const totalDays = Math.floor(totalHours / 24)
  if (totalDays < 7) {
    return `${totalDays}d ago`
  }

  const totalWeeks = Math.floor(totalDays / 7)
  if (totalDays < 30) {
    return `${totalWeeks}w ago`
  }

  const totalMonths = Math.floor(totalDays / 30)
  if (totalDays < 365) {
    return `${totalMonths}mo ago`
  }

  const totalYears = Math.floor(totalDays / 365)
  return `${totalYears}y ago`
}

function setLastUpdated(value) {
  if (!lastUpdated) {
    return
  }

  if (!value) {
    lastUpdated.textContent = "Last refreshed: unavailable"
    return
  }

  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) {
    lastUpdated.textContent = "Last refreshed: unavailable"
    return
  }

  const relative = formatRelativeAge(parsed)
  if (!relative) {
    lastUpdated.textContent = `Last refreshed ${parsed.toISOString()}`
    return
  }

  lastUpdated.textContent = `Last refreshed ${parsed.toISOString()} (${relative})`
}

function setActiveView(view, {updateHistory = false, replaceHistory = false} = {}) {
  viewSections.forEach((section) => {
    section.classList.toggle("hidden", section.dataset.view !== view)
  })

  viewLinks.forEach((link) => {
    const isActive = link.dataset.viewLink === view
    link.classList.toggle("border-neutral-900", isActive)
    link.classList.toggle("font-semibold", isActive)
    link.classList.toggle("text-neutral-900", isActive)
    link.classList.toggle("border-transparent", !isActive)
    link.classList.toggle("text-neutral-500", !isActive)
  })

  const slot = document.querySelector(`[data-view=\"${view}\"] [data-results-slot]`)
  if (slot && resultsSection) {
    slot.appendChild(resultsSection)
  }

  if (resultsTitle) {
    resultsTitle.textContent = view === "snapshot" ? "Snapshot Results" : "Search Results"
  }

  if (updateHistory) {
    const path = buildViewPath(view)
    if (replaceHistory) {
      history.replaceState({view}, "", path)
    } else {
      history.pushState({view}, "", path)
    }
  }
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

async function apiDateRange(type) {
  if (apiMode === "static") {
    const dates = Object.keys(staticManifest?.[type]?.languagesByDate ?? {}).sort()
    const latestDate = staticManifest?.[type]?.latestDate ?? (dates.length > 0 ? dates[dates.length - 1] : null)
    const earliestDate = dates.length > 0 ? dates[0] : null
    return {earliestDate, latestDate}
  }
  return fetchJson(`${apiBase}/date-range?type=${type}`)
}

function parseIsoDateToUtc(value) {
  const parsed = parseIsoDate(value)
  if (!parsed) {
    return null
  }
  return new Date(Date.UTC(parsed.getUTCFullYear(), parsed.getUTCMonth(), parsed.getUTCDate()))
}

function formatDateInput(value) {
  return value.toISOString().slice(0, 10)
}

async function applyRange(range) {
  setMeta(searchMeta, "Updating range...")
  const type = searchType.value
  const rangeData = await apiDateRange(type)
  const latest = rangeData.latestDate
  const earliest = rangeData.earliestDate

  if (!latest) {
    setMeta(searchMeta, "No dates available for this range.", true)
    return
  }

  const latestDate = parseIsoDateToUtc(latest)
  if (!latestDate) {
    setMeta(searchMeta, "Invalid latest date.", true)
    return
  }

  let startDate = latestDate
  if (range === "all") {
    if (!earliest) {
      setMeta(searchMeta, "No earliest date found.", true)
      return
    }
    const earliestDate = parseIsoDateToUtc(earliest)
    if (!earliestDate) {
      setMeta(searchMeta, "Invalid earliest date.", true)
      return
    }
    startDate = earliestDate
  } else {
    const days = Number.parseInt(range, 10)
    if (!Number.isFinite(days) || days <= 0) {
      setMeta(searchMeta, "Invalid range selection.", true)
      return
    }
    startDate = new Date(latestDate.getTime() - (days - 1) * MS_PER_DAY)
    if (earliest) {
      const earliestDate = parseIsoDateToUtc(earliest)
      if (earliestDate && startDate.getTime() < earliestDate.getTime()) {
        startDate = earliestDate
      }
    }
  }

  searchStart.value = formatDateInput(startDate)
  searchEnd.value = formatDateInput(latestDate)
  setActiveRangeButton(range)
  setMeta(searchMeta, `Range set to ${searchStart.value} → ${searchEnd.value}.`)
}

function setActiveRangeButton(range) {
  rangeButtons.forEach((button) => {
    const isActive = button.dataset.range === range
    button.classList.toggle("bg-neutral-900", isActive)
    button.classList.toggle("text-white", isActive)
    button.classList.toggle("border-neutral-900", isActive)
    button.classList.toggle("bg-white", !isActive)
  })
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

  if (!snapshotDate.value) {
    snapshotDate.value = getTodayIso()
  }

  if (repoLatest.date) {
    searchEnd.value = repoLatest.date
  }

  if (repoLatest.date && !searchStart.value) {
    const end = new Date(repoLatest.date)
    const start = new Date(end.getTime() - 6 * MS_PER_DAY)
    searchStart.value = start.toISOString().slice(0, 10)
  }

  if (snapshotType.value === "developer" && !snapshotDate.value && devLatest.date) {
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
  const initialView = getViewFromLocation()
  const hasViewParam = new URLSearchParams(window.location.search).has("view")
  setActiveView(initialView, {updateHistory: hasViewParam, replaceHistory: true})
  await resolveApiMode()
  await loadLastUpdated()
  await populateLatestDates()
  await populateLanguages()
  searchLanguage.value = "(null)"

  viewLinks.forEach((link) => {
    link.addEventListener("click", (event) => {
      event.preventDefault()
      setActiveView(link.dataset.viewLink, {updateHistory: true})
    })
  })

  window.addEventListener("popstate", () => {
    setActiveView(getViewFromLocation())
  })

  rangeButtons.forEach((button) => {
    button.addEventListener("click", async () => {
      await applyRange(button.dataset.range)
    })
  })
}

init().catch((error) => {
  setMeta(snapshotMeta, error.message, true)
})
