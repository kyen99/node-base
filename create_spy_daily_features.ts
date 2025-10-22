import { Command } from 'commander';
import { DateTime } from 'luxon';
import { parse } from 'csv-parse/sync';
import { stringify } from 'csv-stringify/sync';
import fs from 'node:fs/promises';

const ZONE = 'America/New_York';
const FIRST5_TIMES = ['09:30:00', '09:31:00', '09:32:00', '09:33:00', '09:34:00'];
const OUTCOME_START = '09:35:00';
const OUTCOME_END = '11:00:00';
const EPSILON = 1e-8;

interface Bar {
  dt: DateTime;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  barCount: number | null;
  average: number | null;
}

type TouchDirection = 'up' | 'down' | null;

interface DailyRow {
  date: string;
  open_day: number;
  high_5min: number;
  low_5min: number;
  close_5min: number;
  range_5min: number;
  body_5min: number;
  pct_change_5min: number;
  volume_5min: number;
  vwap_5min: number;
  high_to_11am: number;
  low_to_11am: number;
  close_11am: number;
  range_to_11am: number;
  pct_change_to_11am: number;
  pct_move: number;
  direction_peak: number;
  hit_5min_dir: number;
  time_first_touch: TouchDirection;
}

interface ProcessResult {
  dailyRows: DailyRow[];
  daysDroppedFirst5: number;
}

interface RawRecord {
  [key: string]: string;
}

const OUTPUT_COLUMNS: Array<keyof DailyRow> = [
  'date',
  'open_day',
  'high_5min',
  'low_5min',
  'close_5min',
  'range_5min',
  'body_5min',
  'pct_change_5min',
  'volume_5min',
  'vwap_5min',
  'high_to_11am',
  'low_to_11am',
  'close_11am',
  'range_to_11am',
  'pct_change_to_11am',
  'pct_move',
  'direction_peak',
  'hit_5min_dir',
  'time_first_touch',
];

async function readBarsFromFile(inputPath: string): Promise<Bar[]> {
  const content = await fs.readFile(inputPath, 'utf-8');
  return parseBars(content);
}

function parseBars(csvContent: string): Bar[] {
  const records = parse(csvContent, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    bom: true,
  }) as RawRecord[];

  const bars: Bar[] = [];
  for (const record of records) {
    const normalized = normalizeRecord(record);
    const dateStr = normalized.get('date');
    if (!dateStr) {
      continue;
    }

    const timeStr = normalized.get('time') ?? normalized.get('timestamp');
    const dt = parseDateTime(dateStr, timeStr);
    if (!dt) {
      continue;
    }

    const open = parseNumber(normalized.get('open'));
    const high = parseNumber(normalized.get('high'));
    const low = parseNumber(normalized.get('low'));
    const close = parseNumber(normalized.get('close'));
    const volume = parseNumber(normalized.get('volume'));
    const barCount = parseNumber(normalized.get('barcount'));
    const average = parseNumber(normalized.get('average'));

    bars.push({
      dt,
      open,
      high,
      low,
      close,
      volume,
      barCount: Number.isFinite(barCount) ? barCount : null,
      average: Number.isFinite(average) ? average : null,
    });
  }

  return dedupeAndSortBars(bars);
}

function parseNumber(input: string | undefined): number {
  if (input === undefined || input === null) {
    return Number.NaN;
  }
  const trimmed = input.trim();
  if (trimmed === '') {
    return Number.NaN;
  }
  const value = Number(trimmed);
  return Number.isFinite(value) ? value : Number.NaN;
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}

function hasExplicitOffset(value: string): boolean {
  return /([zZ]|[+-]\d{2}:?\d{2})$/.test(value);
}

function normalizeRecord(record: RawRecord): Map<string, string> {
  const normalized = new Map<string, string>();
  for (const [key, value] of Object.entries(record)) {
    if (key === undefined || key === null) {
      continue;
    }
    const normalizedKey = key.trim().toLowerCase();
    if (!normalizedKey) {
      continue;
    }
    if (typeof value === 'string') {
      normalized.set(normalizedKey, value);
    } else if (value !== undefined && value !== null) {
      normalized.set(normalizedKey, String(value));
    }
  }
  return normalized;
}

function parseDateTime(dateRaw: string, timeRaw?: string): DateTime | null {
  const candidates: string[] = [];
  const cleanedDate = dateRaw.trim();
  if (timeRaw && !cleanedDate.includes('T')) {
    candidates.push(`${cleanedDate} ${timeRaw.trim()}`);
  }
  candidates.push(cleanedDate);

  for (const candidate of candidates) {
    const cleaned = candidate.replace(/\s+/g, ' ').trim();
    if (!cleaned) {
      continue;
    }

    const isoCandidate = cleaned.includes('T') ? cleaned : cleaned.replace(' ', 'T');
    let dt = DateTime.fromISO(isoCandidate, { setZone: true });
    if (dt.isValid) {
      dt = hasExplicitOffset(isoCandidate)
        ? dt.setZone(ZONE)
        : dt.setZone(ZONE, { keepLocalTime: true });
      return dt;
    }

    const patterns = [
      'yyyy-LL-dd HH:mm:ss',
      'yyyy-LL-dd HH:mm',
      'yyyyLLdd HH:mm:ss',
      'yyyyLLdd HH:mm',
      'yyyyLLddHH:mm:ss',
      'yyyy-LL-dd',
      'yyyyLLdd',
    ];

    for (const pattern of patterns) {
      dt = DateTime.fromFormat(cleaned, pattern, { zone: ZONE });
      if (dt.isValid) {
        return dt;
      }
    }
  }

  return null;
}

function dedupeAndSortBars(bars: Bar[]): Bar[] {
  const byKey = new Map<string, Bar>();
  for (const bar of bars) {
    const key = String(bar.dt.toMillis());
    byKey.set(key, { ...bar, dt: bar.dt.setZone(ZONE) });
  }
  return Array.from(byKey.values()).sort((a, b) => a.dt.toMillis() - b.dt.toMillis());
}

function processBars(bars: Bar[]): ProcessResult {
  const barsByDate = new Map<string, Bar[]>();
  for (const bar of bars) {
    const local = bar.dt.setZone(ZONE);
    const date = local.toISODate();
    if (!date) {
      continue;
    }
    const list = barsByDate.get(date);
    const normalizedBar = { ...bar, dt: local };
    if (list) {
      list.push(normalizedBar);
    } else {
      barsByDate.set(date, [normalizedBar]);
    }
  }

  const dailyRows: DailyRow[] = [];
  let daysDroppedFirst5 = 0;
  const sortedDates = Array.from(barsByDate.keys()).sort();

  for (const date of sortedDates) {
    const dayBars = barsByDate.get(date);
    if (!dayBars || dayBars.length === 0) {
      continue;
    }
    dayBars.sort((a, b) => a.dt.toMillis() - b.dt.toMillis());
    const { row, droppedFirst5 } = buildDailyRow(date, dayBars);
    if (droppedFirst5) {
      daysDroppedFirst5 += 1;
    }
    if (row) {
      dailyRows.push(row);
    }
  }

  return { dailyRows, daysDroppedFirst5 };
}

function buildDailyRow(date: string, dayBars: Bar[]): { row: DailyRow | null; droppedFirst5: boolean } {
  const barsByTime = new Map<string, Bar>();
  for (const bar of dayBars) {
    barsByTime.set(bar.dt.toFormat('HH:mm:ss'), bar);
  }

  const first5Bars: Bar[] = [];
  for (const time of FIRST5_TIMES) {
    const bar = barsByTime.get(time);
    if (!bar) {
      return { row: null, droppedFirst5: true };
    }
    first5Bars.push(bar);
  }

  if (first5Bars.length !== FIRST5_TIMES.length) {
    return { row: null, droppedFirst5: true };
  }

  const first5Tuple = first5Bars as [Bar, Bar, Bar, Bar, Bar];
  const openDay = first5Tuple[0].open;
  const close5 = first5Tuple[4].close;
  const high5 = computeExtremum(first5Tuple, (bar) => bar.high, true);
  const low5 = computeExtremum(first5Tuple, (bar) => bar.low, false);
  const range5 = computeRange(high5, low5);
  const body5 = Number.isFinite(close5) && Number.isFinite(openDay) ? close5 - openDay : Number.NaN;
  const pctChange5 = Number.isFinite(body5) && Number.isFinite(openDay) && openDay !== 0
    ? body5 / openDay
    : Number.NaN;
  const volume5 = first5Tuple.reduce(
    (acc, bar) => acc + (Number.isFinite(bar.volume) ? bar.volume : 0),
    0,
  );
  const vwap5 = computeVwap(first5Tuple);

  const outcomeBars = dayBars.filter((bar) => {
    const time = bar.dt.toFormat('HH:mm:ss');
    return time >= OUTCOME_START && time <= OUTCOME_END;
  });

  const highTo11 = computeExtremum(outcomeBars, (bar) => bar.high, true);
  const lowTo11 = computeExtremum(outcomeBars, (bar) => bar.low, false);
  const rangeTo11 = computeRange(highTo11, lowTo11);
  const close11 = barsByTime.get('11:00:00')?.close ?? Number.NaN;

  const pctChange11 = Number.isFinite(close11) && Number.isFinite(openDay) && openDay !== 0
    ? (close11 - openDay) / openDay
    : Number.NaN;

  const upExcur = Number.isFinite(highTo11) && Number.isFinite(openDay) && openDay !== 0
    ? (highTo11 - openDay) / openDay
    : Number.NaN;

  const downExcur = Number.isFinite(lowTo11) && Number.isFinite(openDay) && openDay !== 0
    ? (openDay - lowTo11) / openDay
    : Number.NaN;

  let pctMove = Number.NaN;
  if (Number.isFinite(upExcur) && Number.isFinite(downExcur)) {
    pctMove = upExcur >= downExcur ? upExcur : -downExcur;
  }

  let directionPeak = 0;
  if (Number.isFinite(pctMove) && pctMove !== 0) {
    directionPeak = pctMove > 0 ? 1 : -1;
  } else if (
    Number.isFinite(upExcur) &&
    Number.isFinite(downExcur) &&
    upExcur === 0 &&
    downExcur === 0
  ) {
    directionPeak = 0;
  }

  let hit5Dir = 0;
  if (directionPeak !== 0 && Number.isFinite(pctChange5)) {
    const sign5 = Math.sign(pctChange5);
    hit5Dir = sign5 === directionPeak ? 1 : 0;
  }

  const timeFirstTouch = determineFirstTouch(outcomeBars, highTo11, lowTo11);

  const row: DailyRow = {
    date,
    open_day: openDay,
    high_5min: high5,
    low_5min: low5,
    close_5min: close5,
    range_5min: range5,
    body_5min: body5,
    pct_change_5min: pctChange5,
    volume_5min: volume5,
    vwap_5min: vwap5,
    high_to_11am: highTo11,
    low_to_11am: lowTo11,
    close_11am: close11,
    range_to_11am: rangeTo11,
    pct_change_to_11am: pctChange11,
    pct_move: pctMove,
    direction_peak: directionPeak,
    hit_5min_dir: hit5Dir,
    time_first_touch: timeFirstTouch,
  };

  return { row, droppedFirst5: false };
}

function computeExtremum(bars: Bar[], selector: (bar: Bar) => number, isMax: boolean): number {
  if (!bars.length) {
    return Number.NaN;
  }
  let value = isMax ? Number.NEGATIVE_INFINITY : Number.POSITIVE_INFINITY;
  let found = false;
  for (const bar of bars) {
    const candidate = selector(bar);
    if (!Number.isFinite(candidate)) {
      continue;
    }
    if (isMax) {
      if (!found || candidate > value) {
        value = candidate;
        found = true;
      }
    } else if (!found || candidate < value) {
      value = candidate;
      found = true;
    }
  }
  return found ? value : Number.NaN;
}

function computeRange(high: number, low: number): number {
  if (Number.isFinite(high) && Number.isFinite(low)) {
    return high - low;
  }
  return Number.NaN;
}

function computeVwap(bars: Bar[]): number {
  let numerator = 0;
  let denominator = 0;
  for (const bar of bars) {
    if (!Number.isFinite(bar.volume) || bar.volume <= 0) {
      continue;
    }
    const average = bar.average;
    const price = isFiniteNumber(average) ? average : computeTypicalPrice(bar);
    if (!isFiniteNumber(price)) {
      continue;
    }
    numerator += price * bar.volume;
    denominator += bar.volume;
  }
  return denominator > 0 ? numerator / denominator : Number.NaN;
}

function computeTypicalPrice(bar: Bar): number {
  if (Number.isFinite(bar.high) && Number.isFinite(bar.low) && Number.isFinite(bar.close)) {
    return (bar.high + bar.low + bar.close) / 3;
  }
  return Number.NaN;
}

function determineFirstTouch(bars: Bar[], highTarget: number, lowTarget: number): TouchDirection {
  if (!Number.isFinite(highTarget) && !Number.isFinite(lowTarget)) {
    return null;
  }
  let upTouch: DateTime | null = null;
  let downTouch: DateTime | null = null;

  for (const bar of bars) {
    if (upTouch === null && Number.isFinite(highTarget) && Number.isFinite(bar.high)) {
      if (bar.high >= (highTarget as number) - EPSILON) {
        upTouch = bar.dt;
      }
    }
    if (downTouch === null && Number.isFinite(lowTarget) && Number.isFinite(bar.low)) {
      if (bar.low <= (lowTarget as number) + EPSILON) {
        downTouch = bar.dt;
      }
    }
    if (upTouch && downTouch) {
      break;
    }
  }

  if (upTouch && downTouch) {
    if (upTouch.toMillis() < downTouch.toMillis()) {
      return 'up';
    }
    if (downTouch.toMillis() < upTouch.toMillis()) {
      return 'down';
    }
    return null;
  }

  if (upTouch) {
    return 'up';
  }
  if (downTouch) {
    return 'down';
  }
  return null;
}

function buildOutputCsv(rows: DailyRow[]): string {
  return stringify(
    rows.map((row) => OUTPUT_COLUMNS.map((column) => formatOutputValue(row[column]))),
    {
      header: true,
      columns: OUTPUT_COLUMNS,
    },
  );
}

function formatOutputValue(value: DailyRow[keyof DailyRow]): string | number {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : 'NaN';
  }
  if (value === null) {
    return '';
  }
  return value;
}

function printSummary(dailyRows: DailyRow[], daysDroppedFirst5: number): void {
  const pctChange5Values = dailyRows
    .map((row) => row.pct_change_5min)
    .filter(isFiniteNumber);
  const pctMoveValues = dailyRows
    .map((row) => row.pct_move)
    .filter(isFiniteNumber);

  const eligibleHitRows = dailyRows.filter(
    (row) => row.direction_peak !== 0 && Number.isFinite(row.hit_5min_dir),
  );
  const hitRate = eligibleHitRows.length
    ? eligibleHitRows.reduce((acc, row) => acc + row.hit_5min_dir, 0) / eligibleHitRows.length
    : Number.NaN;

  const missingClose11 = dailyRows.filter((row) => !Number.isFinite(row.close_11am)).length;
  const pctMissingClose11 = dailyRows.length
    ? (missingClose11 / dailyRows.length) * 100
    : 0;

  const lines = [
    ['metric', 'value'],
    ['days_processed', String(dailyRows.length)],
    ['days_dropped_first5_incomplete', String(daysDroppedFirst5)],
    ['pct_missing_close_11am', formatNumber(pctMissingClose11, 2)],
    ['mean_pct_change_5min', formatNumber(computeMean(pctChange5Values), 6)],
    ['median_pct_change_5min', formatNumber(computeMedian(pctChange5Values), 6)],
    ['mean_pct_move', formatNumber(computeMean(pctMoveValues), 6)],
    ['median_pct_move', formatNumber(computeMedian(pctMoveValues), 6)],
    ['hit_rate_5min_dir', formatNumber(hitRate, 4)],
  ];

  for (const [metric, value] of lines) {
    console.log(`${metric},${value}`);
  }
}

function computeMean(values: number[]): number {
  if (!values.length) {
    return Number.NaN;
  }
  const total = values.reduce((acc, value) => acc + value, 0);
  return total / values.length;
}

function computeMedian(values: number[]): number {
  if (!values.length) {
    return Number.NaN;
  }
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) {
    const lower = sorted[mid - 1];
    const upper = sorted[mid];
    if (lower === undefined || upper === undefined) {
      return Number.NaN;
    }
    return (lower + upper) / 2;
  }
  const value = sorted[mid];
  return value === undefined ? Number.NaN : value;
}

function formatNumber(value: number, decimals: number): string {
  return Number.isFinite(value) ? value.toFixed(decimals) : 'NaN';
}

async function writeOutput(path: string, content: string): Promise<void> {
  await fs.writeFile(path, content, 'utf-8');
}

function runSyntheticDemo(): void {
  const csv = [
    'date,time,open,high,low,close,volume,barCount,average',
    '2024-01-02,09:30:00,100,100.5,99.5,100.3,1500,300,100.1',
    '2024-01-02,09:31:00,100.3,101,100,100.8,1200,240,100.6',
    '2024-01-02,09:32:00,100.8,101.2,100.5,101,1100,220,100.9',
    '2024-01-02,09:33:00,101,101.4,100.7,101.2,1300,260,101.1',
    '2024-01-02,09:34:00,101.2,101.5,100.8,100.9,1600,320,101.2',
    '2024-01-02,09:35:00,100.9,102,100.7,101.5,1400,280,101.3',
    '2024-01-02,09:40:00,101.5,103,100.5,102.8,1300,260,102.1',
    '2024-01-02,09:50:00,102.8,103.5,100.2,101.2,1500,300,101.6',
    '2024-01-02,10:10:00,101.2,102,99.5,100.1,1600,320,100.5',
    '2024-01-02,10:30:00,100.1,100.8,99.2,99.7,1700,340,99.9',
    '2024-01-02,10:50:00,99.7,100,98.8,99.2,1800,360,99.3',
    '2024-01-02,11:00:00,99.2,99.8,98.5,99,1900,380,99.1',
  ].join('\n');

  const bars = parseBars(csv);
  const { dailyRows, daysDroppedFirst5 } = processBars(bars);
  if (!dailyRows.length) {
    console.log('metric,value');
    console.log('days_processed,0');
    console.log('days_dropped_first5_incomplete,0');
    console.log('pct_missing_close_11am,0.00');
    console.log('mean_pct_change_5min,NaN');
    console.log('median_pct_change_5min,NaN');
    console.log('mean_pct_move,NaN');
    console.log('median_pct_move,NaN');
    console.log('hit_rate_5min_dir,NaN');
    return;
  }

  const output = buildOutputCsv(dailyRows).trim();
  console.log(output);
  printSummary(dailyRows, daysDroppedFirst5);
}

async function main(): Promise<void> {
  const program = new Command();
  program
    .description('Transform 1-minute intraday data into daily feature targets.')
    .option('--in <path>', 'Input CSV file path')
    .option('--out <path>', 'Output CSV file path');

  program.parse(process.argv);
  const options = program.opts<{ in?: string; out?: string }>();

  if (!options.in) {
    runSyntheticDemo();
    return;
  }

  if (!options.out) {
    throw new Error('Missing required --out <path> argument.');
  }

  const bars = await readBarsFromFile(options.in);
  const { dailyRows, daysDroppedFirst5 } = processBars(bars);
  const outputCsv = buildOutputCsv(dailyRows);
  await writeOutput(options.out, outputCsv);
  printSummary(dailyRows, daysDroppedFirst5);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
