package sql

import "time"

type TimeUnit string

const TimeUnitSecond TimeUnit = "SECOND"
const TimeUnitMinute TimeUnit = "MINUTE"
const TimeUnitHour TimeUnit = "HOUR"
const TimeUnitDay TimeUnit = "DAY"

func getTimeUnitWithInterval(duration time.Duration) (unit TimeUnit, interval int64) {
	switch duration {
	case time.Minute:
		unit = "MINUTE"
		interval = int64(duration.Minutes())

	case time.Hour:
		unit = "HOUR"
		interval = int64(duration.Hours())

	case 24 * time.Hour:
		unit = "DAY"
		interval = int64(duration.Hours() / 24)

	default:
		unit = "SECOND"
		interval = int64(duration.Seconds())
	}

	return
}

func timeUnitSql(unit TimeUnit) Expr {
	return Sql(string(unit))
}

func timeUnitString(unit TimeUnit) Expr {
	return String(string(unit))
}
