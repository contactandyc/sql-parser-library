// SPDX-FileCopyrightText: 2024-2026 Andy Curtis <contactandyc@gmail.com>
// SPDX-FileCopyrightText: 2024–2025 Knode.ai — technical questions: contact Andy (above)
// SPDX-License-Identifier: Apache-2.0

#define _XOPEN_SOURCE 700

#include "sql-parser-library/date_utils.h"
#include <ctype.h>

#ifndef HAVE_TIMEGM
time_t custom_timegm(struct tm *tm) {
    time_t local_time = mktime(tm);
    if (local_time == -1) {
        return -1; // Error
    }
    return local_time - timezone; // Adjust for timezone offset
}
#define timegm custom_timegm
#endif

const char *get_timezone(const char *date_str) {
    if(!date_str) {
        return NULL;
    }
    if(strlen(date_str) < 10) {
        return NULL;
    }
    date_str += 10; // Skip the date part
    const char *last_plus = strrchr(date_str, '+');
    const char *last_minus = strrchr(date_str, '-');
    const char *last_Z = strrchr(date_str, 'Z'); // Check for 'Z'
    const char *last_z = strrchr(date_str, 'z'); // Check for 'z'

    if (last_Z && *(last_Z + 1) == '\0') { // Ensure 'Z' is the last character
        return last_Z;
    }

    if (last_z && *(last_z + 1) == '\0') { // Ensure 'z' is the last character
        return last_z;
    }

    if (last_plus && (!last_minus || last_plus > last_minus)) {
        return last_plus;
    } else if (last_minus) {
        return last_minus;
    }
    return NULL;
}

int get_timezone_offset(const char *timezone_part) {
    // Handle the timezone offset if present
    int tz_offset_seconds = 0;
    if (timezone_part) {
        if(timezone_part[0] == 'Z' || timezone_part[0] == 'z') {
            return 0;
        }

        // Restore the sign character in the timezone part
        bool negative = timezone_part[0] == '-';
        timezone_part++;

        // Parse the timezone offset
        int hours = 0, minutes = 0;
        if ((strlen(timezone_part) == 5 && sscanf(timezone_part, "%02d:%02d", &hours, &minutes) == 2) ||
            (strlen(timezone_part) == 4 && sscanf(timezone_part, "%02d%02d", &hours, &minutes) == 2)) {
            tz_offset_seconds = (hours * 3600) + (minutes * 60);
            if (negative) {
                tz_offset_seconds = -tz_offset_seconds;
            }
        } else {
            fprintf(stderr, "Invalid timezone format: %s\n", timezone_part);
            return -1;
        }
    }
    return tz_offset_seconds;
}

bool convert_string_to_datetime(time_t *result, aml_pool_t *pool, const char *date_str) {
    if (!date_str || strlen(date_str) == 0) {
        return false;
    }

    while(isspace(*date_str)) {
        date_str++;
    }

    // check if trailing spaces, if so, copy and trim
    size_t len = strlen(date_str);
    while(len > 0 && isspace(date_str[len - 1])) {
        len--;
    }
    date_str = (char *)aml_pool_strndup(pool, date_str, len);

    char *date_part = (char *)date_str;
    // Copy the input string to a modifiable buffer
    const char *timezone = get_timezone(date_part);
    if(timezone) {
        size_t datetime_len = timezone - date_part;
        date_part = (char *)aml_pool_strndup(pool, date_part, datetime_len);
    }
    int tz_offset_seconds = get_timezone_offset(timezone);
    if(tz_offset_seconds == -1) {
        return false;
    }

    // Determine the format based on the length of the date_part
    size_t date_len = strlen(date_part);
    const char *date_format = NULL;

    if (date_len == 4) {
        date_format = "%Y";
    } else if(date_len > 4 && date_part[4] == '-') {
        if (date_len == 7) {
            date_format = "%Y-%m";
        } else if (date_len == 10) {
            date_format = "%Y-%m-%d";
        } else if (date_len == 13) {
            date_format = "%Y-%m-%dT%H";
        } else if (date_len == 16) {
            date_format = "%Y-%m-%dT%H:%M";
        } else if (date_len == 19) {
            date_format = "%Y-%m-%dT%H:%M:%S";
        } else if (date_len > 19 && date_part[19] == '.') {
            date_format = "%Y-%m-%dT%H:%M:%S";
            date_part[19] = 0;
            date_len = 19;
        } else {
            return false;
        }
    } else if(date_len > 4 && date_part[2] == '-') {
        // mm-yyyy, mm-dd-yyyy, etc
        if (date_len == 7) {
            date_format = "%m-%Y";
        } else if (date_len == 10) {
            date_format = "%m-%d-%Y";
        } else if (date_len == 13) {
            date_format = "%m-%d-%YT%H";
        } else if (date_len == 16) {
            date_format = "%m-%d-%YT%H:%M";
        } else if (date_len == 19) {
            date_format = "%m-%d-%YT%H:%M:%S";
        } else if (date_len > 19 && date_part[19] == '.') {
            date_format = "%m-%d-%YT%H:%M:%S";
            date_part[19] = 0;
        } else {
            return false;
        }
    } else {
        return false;
    }
    if(date_len > 10 && date_part[10] != 'T') {
        date_part[10] = 'T';
    }

    // Parse the date part
    struct tm tm_info = {0};
    char *parse_result = strptime(date_part, date_format, &tm_info);
    if (!parse_result) {
        return false;
    }

    if (date_len == 4) { // Input is only a year (e.g., "2024")
        tm_info.tm_mon = 0;  // January
        tm_info.tm_mday = 1; // 1st day
        tm_info.tm_hour = 0; // Midnight
        tm_info.tm_min = 0;
        tm_info.tm_sec = 0;
    } else if (date_len == 7) { // Input is year and month (e.g., "2024-05")
        tm_info.tm_mday = 1; // Default to 1st day of the month
        tm_info.tm_hour = 0; // Midnight
        tm_info.tm_min = 0;
        tm_info.tm_sec = 0;
    }

    // Convert struct tm to time_t (epoch), assuming the time is in UTC
    time_t epoch = timegm(&tm_info);
    if (epoch == -1) {
        return false;
    }
    // Adjust the epoch time by the timezone offset
    epoch -= tz_offset_seconds;

    *result = epoch;
    return true;
}

char *convert_epoch_to_iso_utc(aml_pool_t *pool, time_t epoch) {
    struct tm tm_info;
    gmtime_r(&epoch, &tm_info); // Convert epoch to UTC time

    // Format the datetime into ISO format with second precision
    char buffer[20]; // "YYYY-MM-DDTHH:MM:SS" is 19 characters + null terminator
    size_t len = strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%S", &tm_info);
    if (len == 0) {
        return NULL;
    }
    return aml_pool_strdup(pool, buffer);
}
