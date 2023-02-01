SELECT *
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
ORDER BY 3,4

-- SELECT *
-- FROM PortfolioProject..CovidVaccinations
-- ORDER BY 3,4

SELECT location, date, total_cases, new_cases, total_deaths, population
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
ORDER BY 1,2

--1. Level Country
-- Death percentage by day
SELECT location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 AS Death_Percentage
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
-- AND location LIKE '%Vietnam%'
ORDER BY 1,2

-- Percentage of population got Covid by day
SELECT location, date, population, total_cases, (total_cases/population)*100 AS Percent_Population_Infected
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
-- AND location LIKE '%Vietnam%'
ORDER BY 1,2

-- Top 5 countries with highest infection rate compared to Population
SELECT TOP(5) location, population, MAX(total_cases) AS Highest_InfectionCount, MAX(total_cases/population)*100 AS Percent_Population_Infected
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY location, population
ORDER BY Percent_Population_Infected DESC

-- Top 5 Countries with highest Death Count per Population
SELECT TOP 5 location, population, MAX(total_deaths) AS Death_Count, (MAX(total_deaths)/population)*100 AS Death_Count_per_Population
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY location, population
ORDER BY Death_Count_per_Population DESC

-- 2. Level Continent
-- Total Death Count of each Continent
WITH CTE_Deathcount AS
(SELECT continent,location, MAX(total_deaths) AS Death_Count_of_Location
FROM PortfolioProject..CovidDeaths 
WHERE continent IS NOT NULL
GROUP BY location,continent)
SELECT continent, SUM(Death_Count_of_Location) AS Death_Count_of_Continent
FROM #Deathcount
GROUP BY continent
ORDER BY 2 DESC

-- Death count per Population
WITH CTE_Deathcount AS
(SELECT continent,location, MAX(total_deaths) AS Death_Count_of_Location, SUM(population) AS Population_of_Location
FROM PortfolioProject..CovidDeaths 
WHERE continent IS NOT NULL
GROUP BY location,continent)
SELECT continent, (SUM(Death_Count_of_Location)/SUM(Population_of_Location))*100 AS Death_Count_Rate
FROM CTE_Deathcount
WHERE continent IS NOT NULL
GROUP BY continent
ORDER BY 2 DESC 

-- 3. Global numbers of death
SELECT date,SUM(new_cases) AS Total_Cases, SUM(new_deaths) AS Total_Deaths, SUM(new_deaths )/SUM(new_cases)*100 AS DeathPercentage
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL 
Group By date
ORDER BY 1,2

-- Percentage of Population that has recieved at least one Covid Vaccine
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(vac.new_vaccinations) OVER (PARTITION BY dea.Location ORDER BY dea.location, dea.Date) AS Rolling_People_Vaccinated, 
((SUM(vac.new_vaccinations) OVER (PARTITION BY dea.Location ORDER BY dea.location, dea.Date))/population)*100 AS Percent_Vaccinated
FROM PortfolioProject..CovidDeaths dea
JOIN PortfolioProject..CovidVaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL 
ORDER BY 2,3

    -- Using CTE to perform Calculation on Partition By in previous query
WITH CTE_PopvsVac (continent, location, date, population, new_vaccinations, Rolling_People_Vaccinated) AS
(SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(vac.new_vaccinations) OVER (PARTITION BY dea.Location ORDER BY dea.location, dea.Date) AS Rolling_People_Vaccinated
FROM PortfolioProject..CovidDeaths dea
JOIN PortfolioProject..CovidVaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL )
SELECT *, (Rolling_People_Vaccinated/population)*100 AS Percent_Vaccinated
FROM CTE_PopvsVac

    -- Using Temp Table to perform Calculation on Partition By in previous query
DROP TABLE IF EXISTS #PopvsVac
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(vac.new_vaccinations) OVER (PARTITION BY dea.Location ORDER BY dea.location, dea.Date) AS Rolling_People_Vaccinated
INTO #PopvsVac
FROM PortfolioProject..CovidDeaths dea
JOIN PortfolioProject..CovidVaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
SELECT *, (Rolling_People_Vaccinated/population)*100 AS Percent_Vaccinated
FROM #PopvsVac

-- Creating View to store data for later visualizations
CREATE VIEW Percent_Population_Vaccinated AS
SELECT dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(vac.new_vaccinations) OVER (PARTITION BY dea.Location ORDER BY dea.location, dea.Date) AS Rolling_People_Vaccinated
--, (RollingPeopleVaccinated/population)*100
FROM PortfolioProject..CovidDeaths dea
JOIN PortfolioProject..CovidVaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL 

