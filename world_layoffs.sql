SELECT *
FROM layoffs;

CREATE TABLE layoff_staging
SELECT *
FROM layoffs;

ALTER table layoff_staging
RENAME TO layoffs_staging;

WITH cte_layoffs_staging AS
(
SELECT *,
ROW_NUMBER() OVER (PARTITION BY company,location,industry,total_laid_off,`date`,stage,country,funds_raised_millions) row_num
FROM layoffs_staging 
)
SELECT * 
FROM cte_layoffs_staging
WHERE row_num>1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT * ,
ROW_NUMBER() OVER (PARTITION BY company,location,industry,total_laid_off,`date`,stage,country,funds_raised_millions) row_num
FROM layoffs_staging ;

SELECT * 
FROM layoffs_staging2 
WHERE row_num>1 ;

DELETE FROM 
layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM
layoffs_staging2;

SELECT company,TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT(location)
FROM layoffs_staging2;

SELECT industry
FROM layoffs_staging2
WHERE industry LIKE 'crypto%' ;

UPDATE layoffs_staging2
SET industry = 'crypto'
WHERE industry LIKE 'crypto%';

SELECT DISTINCT(country),TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
WHERE country LIKE 'UNITED STATE%';

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'UNITED STATE%';

SELECT DISTINCT(country)
FROM layoffs_staging2
WHERE country LIKE 'UNITED STATE%';

SELECT * 
FROM layoffs_staging2;

SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE company AND location AND industry AND total_laid_off AND percentage_laid_off AND date AND stage AND country AND funds_raised_millions AND row_num
IS NULL;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY `date` date ; 

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT industry,company
FROM  layoffs_staging2
ORDER BY 1;

SELECT company,industry 
FROM layoffs_staging2
WHERE industry = '' OR NULL;

SELECT t1.industry,t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company= t2.company
WHERE( t1.industry IS NULL OR t1.industry = '')
AND (t2.industry IS NOT NULL OR t1.industry <> '');

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company= t2.company
SET t1.company= t2.company
WHERE( t1.industry IS NULL )
AND (t2.industry IS NOT NULL )