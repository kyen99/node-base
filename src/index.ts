import Papa from 'papaparse'
import fs from 'fs'

type Data = {
  date: string
  time: string
  open: number
  high: number
  low: number
  close: number
  volume: number
}

type Res = {
  data: Data[]
  errors: any[]
  meta: any
}

const convertEuDateToUs = (d: Data) => ({
  ...d,
  date: d.date?.replace(/(\d{2})\/(\d{2})\/(\d{4})/, '$2/$1/$3'),
})

const removeWeekends = (d: Data) =>
  new Date(d.date).getDay() !== 0 && new Date(d.date).getDay() !== 6

const getDatesAfterYear = (year: number) => (d: Data) =>
  new Date(d.date).getFullYear() >= year

const complete = (res: Res) => {
  const fullDates = res.data
    .filter((d: Data) => d.time === '00:00:00')
    .map((d: Data) => d.date)

  const cleanData = res.data
    .map(convertEuDateToUs)
    .filter(removeWeekends)
    .filter(getDatesAfterYear(2007))
    .filter((d: Data) => fullDates.includes(d.date))

  const augData = cleanData.map((d, idx) => ({
    ...d,
    // ma96:
    //   idx < 96
    //     ? d.close
    //     : cleanData.slice(idx - 96, idx + 1).reduce((a, c) => a + c.close, 0) /
    //       96,
  }))

  const dates = [...new Set(augData.map((d) => d.date))]

  const getFields = (date: string) => {
    const dateData = augData
      .filter((d) => d.date === date)
      .filter((d) =>
        [
          '07:30:00',
          '07:45:00',
          '08:00:00',
          '08:15:00',
          '08:30:00',
          '08:45:00',
          '09:00:00',
          '09:15:00',
          '09:30:00',
          '09:45:00',
          '10:00:00',
          '10:15:00',
          '10:30:00',
          '10:45:00',
          '14:45:00',
        ].includes(d.time)
      )
    let cumulativeVolume = 0
    let cumulativePriceVolume = 0
    return dateData
      .map((d) => {
        const diff = (d.close - d.open) / d.open
        cumulativeVolume += d.volume
        cumulativePriceVolume += d.volume * ((d.high + d.low + d.close) / 3)

        const vwap = cumulativePriceVolume / cumulativeVolume

        return {
          [`${d.time}_open`]: d.open,
          [`${d.time}_high`]: d.high,
          [`${d.time}_low`]: d.low,
          [`${d.time}_close`]: d.close,
          [`${d.time}_vol`]: d.volume,
          [`${d.time}_range`]: (d.high - d.low) / d.open,
          [`${d.time}_diff`]: diff,
          [`${d.time}_vwap`]: vwap,
        }
      })
      .reduce((a, c) => {
        return { ...a, ...c }
      }, {})
  }

  const dateMap = dates
    .map((date, idx) => {
      return {
        date,
        ...getFields(date),
      }
    })
    .reduce((a, c) => {
      if (Object.keys(c).length < 121) return a
      return [...a, c]
    }, [] as any[])

  const csv = Papa.unparse(dateMap)
  fs.writeFileSync('./es-15m-pivot.csv', csv)
}

const fileData = fs.readFileSync('./es-15m.csv', 'utf8')

const data = Papa.parse(fileData, {
  delimiter: ';',
  header: true,
  dynamicTyping: true,
  complete,
})
