-- Data cleaning
select *
from layoffs;
-- step 1. remove dupilcate 
-- step2. standardize the data
-- step3. null vlaues or black values
-- step4. remove any colums


create table layoffs_staging2 
like layoffs;
with duplicate_cte as(
select *,
row_number() over (partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging
)

insert layoffs_staging
select *
from layoffs;
-- step1: 

select * 
from duplicate_cte
where row_num >1;





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

insert into layoffs_staging2
select *,
row_number() over (
partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging;
select *
from layoffs_staging2
where row_num >1;
delete
from layoffs_staging2
where row_num >1;
select *
from layoffs_staging2
where row_num >1;
-- step2 standardizing data
select company,trim(company)
from layoffs_staging2;
update layoffs_staging2
set company=trim(company);
select distinct industry
from layoffs_staging2
order by 1;
select *
from layoffs_staging2
where industry like 'crypto%';
update layoffs_staging2
set industry='crypto'
where industry like 'crypto%';
select distinct country
from layoffs_staging2
order by  1;
select *
from layoffs_staging2
where country like 'United States%';
select distinct country,trim(trailing '.' from  country)
from layoffs_staging2
order by 1;
update layoffs_staging2
set country=trim(trailing '.' from  country)
where country like 'United States%';

-- change the data format
select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;
update layoffs_staging2
set `date`=str_to_date(`date`,'%m/%d/%Y');
alter table layoffs_staging2
modify column `date` Date;

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null or industry='';
update layoffs_staging2 
set industry=null
where industry='';
select t1.industry,t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
   on t1.company=t2.company
   and t1.location=t2.location
   where (t1.industry is null or t1.industry='')
   and t2.industry is not null;   
   
   update layoffs_staging2 t1
   join layoffs_staging2 t2
	on t1.company=t2.company
set t1.industry=t2.industry
where t1.industry is null 
and t2.industry is not null;
select *
from layoffs_staging2;  
select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null or industry='';

delete 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null or industry='';


select *
from layoffs_staging2; 

  
alter table layoffs_staging2
drop column row_num;


-- DATA EXPLORATORY DATA ANALYSIS 
select max(total_laid_off),max(percentage_laid_off)
from layoffs_staging2;
select *
from layoffs_staging2
where percentage_laid_off=1
order by total_laid_off desc;

select company,sum(total_laid_off)
from layoffs_staging2
group by company 
order by 2 desc;


select min(`date`),max(`date`)
from layoffs_staging2;

select country ,sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;


select Year(`date`) ,sum(total_laid_off)
from layoffs_staging2
group by Year(`date`)
order by 2 desc;


select country ,avg(percentage_laid_off)
from layoffs_staging2
group by country
order by 2 desc;


select substring(`date`,1,7) as `month`,sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc;


with rolling_total as(
select substring(`date`,1,7) as `month`,sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc)
select `month` ,sum(total_off) over(order by `month`)
from rolling_total;


 
 
 with company_year(company,years,total_laid_off) as(
select company,Year(`date`),sum(total_laid_off)
from layoffs_staging2
group by company,Year(`date`))
select *,dense_rank() over(partition by years order by total_laid_off desc)
from company_year;