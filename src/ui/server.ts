import http from "http"
import path from "path"
import fs from "fs/promises"
import {createReadStream} from "fs"

type UiServerOptions = {
    archiveRoot: string
    port: number
    bind: string
}

type ApiError = {
    error: string
}

type SearchResult = {
    date: string
    language: string | null
    rank: number
    name: string
}

const CONTENT_TYPES: Record<string, string> = {
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".json": "application/json; charset=utf-8",
}

const MAX_SEARCH_DAYS = 370

const latestCache = new Map<string, string | null>()
const lastUpdatedCache = new Map<string, string | null>()

export async function startUiServer(options: UiServerOptions) {
    const archiveRoot = path.resolve(options.archiveRoot)
    const uiRoot = path.resolve(__dirname, "../../web")

    await ensureArchiveRoot(archiveRoot)
    await ensureUiRoot(uiRoot)

    const server = http.createServer(async (req, res) => {
        if (!req.url) {
            sendJson(res, 400, {error: "Missing URL"})
            return
        }

        const requestUrl = new URL(req.url, `http://${options.bind}:${options.port}`)

        if (requestUrl.pathname.startsWith("/api/")) {
            try {
                await handleApi(requestUrl, archiveRoot, res)
            } catch (error) {
                sendJson(res, 500, {error: (error as Error).message})
            }
            return
        }

        await serveStatic(uiRoot, requestUrl.pathname, res)
    })

    return new Promise<void>((resolve) => {
        server.listen(options.port, options.bind, () => {
            console.log(`UI server running at http://${options.bind}:${options.port}`)
            resolve()
        })
    })
}

async function ensureArchiveRoot(archiveRoot: string) {
    const repositoryRoot = path.join(archiveRoot, "repository")
    const developerRoot = path.join(archiveRoot, "developer")

    await fs.access(repositoryRoot)
    await fs.access(developerRoot)
}

async function ensureUiRoot(uiRoot: string) {
    await fs.access(path.join(uiRoot, "index.html"))
}

async function handleApi(requestUrl: URL, archiveRoot: string, res: http.ServerResponse) {
    switch (requestUrl.pathname) {
        case "/api/health":
            sendJson(res, 200, {ok: true})
            return
        case "/api/latest":
            await handleLatest(requestUrl, archiveRoot, res)
            return
        case "/api/languages":
            await handleLanguages(requestUrl, archiveRoot, res)
            return
        case "/api/snapshot":
            await handleSnapshot(requestUrl, archiveRoot, res)
            return
        case "/api/search":
            await handleSearch(requestUrl, archiveRoot, res)
            return
        case "/api/last-updated":
            await handleLastUpdated(archiveRoot, res)
            return
        default:
            sendJson(res, 404, {error: "Unknown API route"})
    }
}

async function handleLatest(requestUrl: URL, archiveRoot: string, res: http.ServerResponse) {
    const type = requestUrl.searchParams.get("type")
    if (!type || (type !== "repository" && type !== "developer")) {
        sendJson(res, 400, {error: "type must be repository or developer"})
        return
    }

    const cacheKey = `${archiveRoot}:${type}`
    if (latestCache.has(cacheKey)) {
        sendJson(res, 200, {date: latestCache.get(cacheKey)})
        return
    }

    const latest = await findLatestDate(path.join(archiveRoot, type))
    latestCache.set(cacheKey, latest)
    sendJson(res, 200, {date: latest})
}

async function handleLanguages(requestUrl: URL, archiveRoot: string, res: http.ServerResponse) {
    const type = requestUrl.searchParams.get("type")
    const date = requestUrl.searchParams.get("date")

    if (!type || (type !== "repository" && type !== "developer")) {
        sendJson(res, 400, {error: "type must be repository or developer"})
        return
    }

    if (!date || !isIsoDate(date)) {
        sendJson(res, 400, {error: "date must be YYYY-MM-DD"})
        return
    }

    const languages = await listLanguages(path.join(archiveRoot, type), date)
    sendJson(res, 200, {date, languages})
}

async function handleSnapshot(requestUrl: URL, archiveRoot: string, res: http.ServerResponse) {
    const type = requestUrl.searchParams.get("type")
    const date = requestUrl.searchParams.get("date")
    const language = normalizeLanguage(requestUrl.searchParams.get("language"))

    if (!type || (type !== "repository" && type !== "developer")) {
        sendJson(res, 400, {error: "type must be repository or developer"})
        return
    }

    if (!date || !isIsoDate(date)) {
        sendJson(res, 400, {error: "date must be YYYY-MM-DD"})
        return
    }

    const filePath = buildArchiveFilePath(archiveRoot, type, date, language)
    const payload = await readJson(filePath)
    sendJson(res, 200, payload)
}

async function handleSearch(requestUrl: URL, archiveRoot: string, res: http.ServerResponse) {
    const type = requestUrl.searchParams.get("type")
    const query = requestUrl.searchParams.get("query")
    const start = requestUrl.searchParams.get("start")
    const end = requestUrl.searchParams.get("end")
    const language = normalizeLanguage(requestUrl.searchParams.get("language"))
    const limitValue = requestUrl.searchParams.get("limit")

    if (!type || (type !== "repository" && type !== "developer")) {
        sendJson(res, 400, {error: "type must be repository or developer"})
        return
    }

    if (!query || query.trim().length < 2) {
        sendJson(res, 400, {error: "query must be at least 2 characters"})
        return
    }

    if (!start || !end || !isIsoDate(start) || !isIsoDate(end)) {
        sendJson(res, 400, {error: "start/end must be YYYY-MM-DD"})
        return
    }

    const startDate = parseIsoDate(start)
    const endDate = parseIsoDate(end)
    if (!startDate || !endDate || startDate.getTime() > endDate.getTime()) {
        sendJson(res, 400, {error: "invalid date range"})
        return
    }

    const rangeDays = Math.floor((endDate.getTime() - startDate.getTime()) / msPerDay()) + 1
    if (rangeDays > MAX_SEARCH_DAYS) {
        sendJson(res, 400, {error: `date range too large (max ${MAX_SEARCH_DAYS} days)`})
        return
    }

    const limit = limitValue ? Number.parseInt(limitValue, 10) : 200
    const safeLimit = Number.isFinite(limit) && limit > 0 ? Math.min(limit, 500) : 200

    const results = await searchArchive({
        archiveRoot,
        type,
        query,
        startDate,
        endDate,
        language,
        limit: safeLimit,
    })

    sendJson(res, 200, {
        count: results.length,
        results,
    })
}

async function handleLastUpdated(archiveRoot: string, res: http.ServerResponse) {
    const cacheKey = `last-updated:${archiveRoot}`
    if (lastUpdatedCache.has(cacheKey)) {
        sendJson(res, 200, {updatedAt: lastUpdatedCache.get(cacheKey)})
        return
    }

    const updatedAt = await findLatestUpdatedAt(archiveRoot)
    lastUpdatedCache.set(cacheKey, updatedAt)
    sendJson(res, 200, {updatedAt})
}

async function searchArchive(params: {
    archiveRoot: string
    type: "repository" | "developer"
    query: string
    startDate: Date
    endDate: Date
    language: string
    limit: number
}): Promise<SearchResult[]> {
    const results: SearchResult[] = []
    const normalizedQuery = params.query.trim().toLowerCase()

    for (const date of enumerateDates(params.startDate, params.endDate)) {
        const dateValue = formatIsoDate(date)
        const filePath = buildArchiveFilePath(params.archiveRoot, params.type, dateValue, params.language)

        const payload = await readJsonIfExists(filePath)
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

            if (results.length >= params.limit) {
                return results
            }
        }
    }

    return results
}

async function listLanguages(typeRoot: string, date: string): Promise<string[]> {
    const year = date.slice(0, 4)
    const dirPath = path.join(typeRoot, year, date)

    const entries = await fs.readdir(dirPath, {withFileTypes: true})
    return entries
        .filter((entry) => entry.isFile() && entry.name.endsWith(".json"))
        .map((entry) => entry.name.replace(/\.json$/, ""))
        .sort()
}

async function findLatestDate(typeRoot: string): Promise<string | null> {
    const yearDirs = await fs.readdir(typeRoot, {withFileTypes: true})
    const years = yearDirs
        .filter((entry) => entry.isDirectory())
        .map((entry) => entry.name)
        .filter((name) => /^\d{4}$/.test(name))
        .sort()
        .reverse()

    for (const year of years) {
        const dateDirs = await fs.readdir(path.join(typeRoot, year), {withFileTypes: true})
        const dates = dateDirs
            .filter((entry) => entry.isDirectory())
            .map((entry) => entry.name)
            .filter((name) => isIsoDate(name))
            .sort()
            .reverse()

        if (dates.length > 0) {
            return dates[0]
        }
    }

    return null
}

async function findLatestUpdatedAt(archiveRoot: string): Promise<string | null> {
    const typeRoots = ["repository", "developer"].map((type) => path.join(archiveRoot, type))
    let latestMtimeMs: number | null = null

    for (const typeRoot of typeRoots) {
        let yearDirs
        try {
            yearDirs = await fs.readdir(typeRoot, {withFileTypes: true})
        } catch {
            continue
        }

        const years = yearDirs
            .filter((entry) => entry.isDirectory())
            .map((entry) => entry.name)
            .filter((name) => /^\d{4}$/.test(name))

        for (const year of years) {
            const yearRoot = path.join(typeRoot, year)
            const dateDirs = await fs.readdir(yearRoot, {withFileTypes: true})
            const dates = dateDirs
                .filter((entry) => entry.isDirectory())
                .map((entry) => entry.name)
                .filter((name) => isIsoDate(name))

            for (const date of dates) {
                const dayRoot = path.join(yearRoot, date)
                const dayEntries = await fs.readdir(dayRoot, {withFileTypes: true})
                for (const entry of dayEntries) {
                    if (!entry.isFile() || !entry.name.endsWith(".json")) {
                        continue
                    }

                    const filePath = path.join(dayRoot, entry.name)
                    const stat = await fs.stat(filePath)
                    if (latestMtimeMs === null || stat.mtimeMs > latestMtimeMs) {
                        latestMtimeMs = stat.mtimeMs
                    }
                }
            }
        }
    }

    if (latestMtimeMs === null) {
        return null
    }

    return new Date(latestMtimeMs).toISOString()
}

async function serveStatic(uiRoot: string, pathname: string, res: http.ServerResponse) {
    const requestedPath = pathname === "/" ? "/index.html" : pathname
    const safePath = path.normalize(requestedPath).replace(/^\.\.(\/|\\)/, "")
    const filePath = path.join(uiRoot, safePath)

    try {
        const stat = await fs.stat(filePath)
        if (!stat.isFile()) {
            sendJson(res, 404, {error: "Not found"})
            return
        }

        const contentType = CONTENT_TYPES[path.extname(filePath)] ?? "application/octet-stream"
        res.writeHead(200, {"Content-Type": contentType})
        createReadStream(filePath).pipe(res)
    } catch {
        sendJson(res, 404, {error: "Not found"})
    }
}

function buildArchiveFilePath(archiveRoot: string, type: string, date: string, language: string) {
    const year = date.slice(0, 4)
    return path.join(archiveRoot, type, year, date, `${language}.json`)
}

function normalizeLanguage(language: string | null): string {
    if (!language || language === "all") {
        return "(null)"
    }

    return language
}

function sendJson(res: http.ServerResponse, status: number, payload: object | ApiError) {
    res.writeHead(status, {"Content-Type": "application/json; charset=utf-8"})
    res.end(JSON.stringify(payload))
}

async function readJson(filePath: string) {
    const raw = await fs.readFile(filePath, "utf-8")
    return JSON.parse(raw)
}

async function readJsonIfExists(filePath: string) {
    try {
        return await readJson(filePath)
    } catch {
        return null
    }
}

function isIsoDate(value: string) {
    return /^\d{4}-\d{2}-\d{2}$/.test(value)
}

function parseIsoDate(value: string): Date | null {
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

function formatIsoDate(date: Date): string {
    return [date.getUTCFullYear(), date.getUTCMonth() + 1, date.getUTCDate()]
        .map((value) => String(value).padStart(2, "0"))
        .join("-")
}

function msPerDay() {
    return 24 * 60 * 60 * 1000
}

function* enumerateDates(start: Date, end: Date) {
    const startTime = Date.UTC(start.getUTCFullYear(), start.getUTCMonth(), start.getUTCDate())
    const endTime = Date.UTC(end.getUTCFullYear(), end.getUTCMonth(), end.getUTCDate())

    for (let time = startTime; time <= endTime; time += msPerDay()) {
        yield new Date(time)
    }
}
