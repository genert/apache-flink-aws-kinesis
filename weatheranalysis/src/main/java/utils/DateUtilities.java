package utils;

import java.time.*;
import java.util.Date;

public class DateUtilities {

    public static Date from(LocalDate localDate, LocalTime localTime, ZoneOffset zoneOffset) {
        LocalDateTime localDateTime = localDate.atTime(localTime);

        return from(localDateTime, zoneOffset);
    }

    public static Date from(LocalDateTime localDateTime, ZoneOffset zoneOffset) {
        OffsetDateTime offsetDateTime = localDateTime.atOffset(zoneOffset);

        return Date.from(offsetDateTime.toInstant());
    }

}