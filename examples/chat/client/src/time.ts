const timeFormatter = new Intl.DateTimeFormat([], { hour: "2-digit", minute: "2-digit" })

export const formatTime = (millis: number): string => timeFormatter.format(millis)
