-- Data Cleaning

Select * from layoffs;


-- Duplicate Table
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * FROM layoffs_staging;

INSERT layoffs_staging
SELECT * 
FROM layoffs;


-- REMOVE DUPLICATES
SELECT *,
row_number() OVER(
	partition by  company, industry, location, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) row_num
FROM layoffs_staging;

WITH dumpicate_cte AS
(
SELECT *,
row_number() OVER(
	partition by  company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) row_num
FROM layoffs_staging
)
SELECT *
FROM dumpicate_cte
WHERE row_num > 1;




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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



SELECT * FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
row_number() OVER(
	partition by  company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num >1;


-- STANDARDIZING the DATA
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT distinct(industry)
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'crypto%';

SELECT distinct(location)
FROM layoffs_staging2
ORDER BY 1;

SELECT distinct(country)
FROM layoffs_staging2
ORDER BY 1;

SELECT distinct(country), TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

select `date`,  str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2 
modify COLUMN `date` DATE;






-- NULL VALUES OR BLANK VALUES    
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
	OR industry = '';
    
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

UPDATE layoffs_staging2
SET industry = null
WHERE industry = '';

SELECT  t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL 
;


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2. industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;





-- REMOVE ANY COLUMNS OR ROWS    
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
	AND percentage_laid_off IS NULL;
    
DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
	AND percentage_laid_off IS NULL;
    
SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP column  row_num;




 -- EXPLORING DATA
 SELECT *
FROM layoffs_staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1 
ORDER BY funds_raised_millions DESC;


SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;


SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;



SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 1 DESC;


SELECT *
FROM layoffs_staging2;

SELECT company, AVG(percentage_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;


SELECT DATE_FORMAT(date, '%Y-%m') as `Month`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE DATE_FORMAT(date, '%Y-%m') IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC;


WITH rolling_layoff AS
(
SELECT DATE_FORMAT(date, '%Y-%m') as `Month`, SUM(total_laid_off) as total_off
FROM layoffs_staging2
WHERE DATE_FORMAT(date, '%Y-%m') IS NOT NULL
GROUP BY `Month`
ORDER BY 1 ASC
)
SELECT *, SUM(total_off) OVER (ORDER BY `Month`) AS rolling_total
FROM rolling_layoff;


SELECT company,YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,YEAR(`date`)
ORDER BY 3 DESC;

WITH company_year (company, years, total_laid_off) AS
(
SELECT company,YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company,YEAR(`date`)
), Company_Year_Rank AS (
SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) as Rangking
FROM company_year
WHERE years IS NOT NULL
)
SELECT * 
FROM Company_Year_Rank
WHERE Rangking <= 5
ORDER BY Rangking ASC;