# -*- coding: utf-8 -*-

# below are configs for downloader when downloading the source files.
MEASURES = [1]  # deaths
METRICS = [3]   # rate
# age-standarized data: age = 27. All age data: age = 22
# But we only need age-standarized when metric = 3.
AGES = [22, 27, 1, 6, 7, 8, 9, 10, 11, 12, 13, 14,
        15, 16, 17, 18, 19, 20, 30, 31, 32, 235, 23]

YEARS = list(range(1990, 2018))
LOCATIONS = [10, 101, 102, 105, 106, 107, 108, 109, 11, 110, 111,
             112, 113, 114, 115, 116, 117, 118, 119, 12, 121, 122,
             123, 125, 126, 127, 128, 129, 13, 130, 131, 132, 133,
             135, 136, 139, 14, 140, 141, 142, 143, 144, 145, 146,
             147, 148, 149, 15, 150, 151, 152, 153, 154, 155, 156,
             157, 16, 160, 161, 162, 163, 164, 165, 168, 169, 17,
             170, 171, 172, 173, 175, 176, 177, 178, 179, 18, 180,
             181, 182, 183, 184, 185, 186, 187, 189, 19, 190, 191,
             193, 194, 195, 196, 197, 198, 20, 200, 201, 202, 203,
             204, 205, 206, 207, 208, 209, 210, 211, 212, 213,
             214, 215, 216, 217, 218, 22, 23, 24, 25, 26, 27, 28,
             29, 298, 30, 305, 33, 34, 349, 35, 351, 36, 37, 376,
             38, 385, 39, 40, 41, 422, 43, 435, 44, 45, 46, 47, 48,
             49, 50, 51, 52, 522, 53, 533, 54, 55, 57, 58, 59, 6,
             60, 61, 62, 63, 66, 67, 68, 69, 7, 71, 72, 74, 75, 76,
             77, 78, 79, 8, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89,
             90, 91, 92, 93, 94, 95, 97, 98, 99]

SEXES = [1, 2, 3]