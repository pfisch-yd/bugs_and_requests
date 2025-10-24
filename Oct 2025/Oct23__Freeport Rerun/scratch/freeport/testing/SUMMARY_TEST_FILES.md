# Sumário dos Arquivos de Teste - Freeport Geo Analysis

## Arquivos Analisados

### 1. **00_Basic query.py**
- **Arquivo Run Geo:** `after__geo2`
- **Test Client:** `testesteelauder`
- **Tabelas Referenciadas:**
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.testesteelauder_v38_geographic_analysis5__dmv__000`

---

### 2. **00_Basic query__generac.py**
- **Arquivo Run Geo:** `after__geo2`
- **Test Client:** `generac`
- **Tabelas Referenciadas:**
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.testesteelauder_v38_geographic_analysis5__dmv__000`
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.werner_v38_geographic_analysis5__dmv__000`

---

### 3. **00_Basic query__graco.py**
- **Arquivo Run Geo:** `after__geo2`
- **Test Client:** `testgraco`
- **Tabelas Referenciadas:**
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.testesteelauder_v38_geographic_analysis5__dmv__000`

---

### 4. **00_Basic query__ove.py**
- **Arquivo Run Geo:** `after__geo2`
- **Test Client:** `ove`
- **Tabelas Referenciadas:**
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.testesteelauder_v38_geographic_analysis5__dmv__000`

---

### 5. **00_Basic query__renin.py**
- **Arquivo Run Geo:** `after__geo2`
- **Test Client:** `renin`
- **Tabelas Referenciadas:**
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.testesteelauder_v38_geographic_analysis5__dmv__000`
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.renin_v38_geographic_analysis5__dmv__000`

---

### 6. **00_Basic query__werner.py**
- **Arquivo Run Geo:** `after__geo2`
- **Test Client:** `werner`
- **Tabelas Referenciadas:**
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.testesteelauder_v38_geographic_analysis5__dmv__000`
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.werner_v38_geographic_analysis5__dmv__000`

---

### 7. **00__creating more tests.py**
- **Arquivo Run Geo:** `after__geo2`
- **Test Client:** `werner`, `ove`, `renin`, `generac` (múltiplos)
- **Tabelas Referenciadas:** Nenhuma após freeport_geo_analysis()

---

### 8. **01_add new column__ove.py**
- **Arquivo Run Geo:** `after__geo2__add column test`
- **Test Client:** `ove`
- **Tabelas Referenciadas:**
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.ove_v38_geographic_analysis5__dmv__000`
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.testesteelauder_v38_geographic_analysis5__dmv__000`

---

### 9. **01_add new column__testestee.py**
- **Arquivo Run Geo:** `after__geo2__add column test`
- **Test Client:** `testesteelauder`
- **Tabelas Referenciadas:**
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.testesteelauder_v38_geographic_analysis5__dmv__000`

---

### 10. **02_change data type__renin.py**
- **Arquivo Run Geo:** `after__geo2`
- **Test Client:** `renin`
- **Tabelas Referenciadas:**
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.renin_v38_geographic_analysis5__dmv__001`
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.testgraco_v38_geographic_analysis5__dmv__001`
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.testgraco_v38_geographic_analysis5__dmv__002`

---

### 11. **02_change data type__testestee.py**
- **Arquivo Run Geo:** `after__geo2`
- **Test Client:** `testesteelauder`
- **Tabelas Referenciadas:**
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.renin_v38_geographic_analysis5__dmv__001`
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.testgraco_v38_geographic_analysis5__dmv__001`
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.testgraco_v38_geographic_analysis5__dmv__002`

---

### 12. **03_drop a column__werner.py**
- **Arquivo Run Geo:** `after__geo2__drop a column test`
- **Test Client:** `werner`
- **Tabelas Referenciadas:**
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.werner_v38_geographic_analysis5__dmv__001`
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.werner_v38_geographic_analysis5__dmv__000`
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.testesteelauder_v38_geographic_analysis5__dmv__000`

---

### 13. **04_change column order__generac.py**
- **Arquivo Run Geo:** `after__geo2__change column order`
- **Test Client:** `generac`
- **Tabelas Referenciadas:**
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.ove_v38_geographic_analysis5__dmv__000`
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.testesteelauder_v38_geographic_analysis5__dmv__000`

---

### 14. **04_change column order__ove.py**
- **Arquivo Run Geo:** `after__geo2__change column order`
- **Test Client:** `ove`
- **Tabelas Referenciadas:**
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.ove_v38_geographic_analysis5__dmv__000`
  - `yd_fp_corporate_staging.ydx_internal_analysts_gold.testesteelauder_v38_geographic_analysis5__dmv__000`

---

### 15. **DMV___001 issue.py**
- **Arquivo Run Geo:** `after__geo2`
- **Test Client:** `testesteelauder`, `testgraco` (múltiplos)
- **Tabelas Referenciadas:** Nenhuma após freeport_geo_analysis()
- **Nota:** Este arquivo investiga o problema de versionamento DMV (000 vs 001)

---

### 16. **Intentional Schema Change.py**
- **Arquivo Run Geo:** `after__geo2`, `after__geo3`
- **Test Client:** `testesteelauder`
- **Tabelas Referenciadas:** Nenhuma após freeport_geo_analysis()
- **Nota:** Testa mudanças intencionais de schema (remoção de colunas, adição de novas colunas)

---

### 17. **Unintentional Schema Chaneg.py**
- **Arquivo Run Geo:** `after__geo2`
- **Test Client:** `testgraco`
- **Tabelas Referenciadas:** Nenhuma após freeport_geo_analysis()
- **Nota:** Testa mudanças não intencionais de schema

---

## Resumo Geral

### Arquivos Run Geo Utilizados:
- `after__geo2` (maioria)
- `after__geo2__add column test`
- `after__geo2__drop a column test`
- `after__geo2__change column order`
- `after__geo3`

### Test Clients Utilizados:
- `testesteelauder`
- `testgraco`
- `ove`
- `renin`
- `werner`
- `generac`

### Padrão de Tabelas:
Todas as tabelas seguem o padrão:
```
yd_fp_corporate_staging.ydx_internal_analysts_gold.{client}_v38_geographic_analysis5__dmv__{version}
```

Onde:
- `{client}` = nome do cliente (ove, werner, renin, generac, testesteelauder, testgraco)
- `{version}` = versão DMV (000, 001, 002, etc.)

### Tipos de Testes Realizados:
1. **Basic query** - Query básica sem modificações
2. **Add new column** - Adicionar nova coluna ao schema
3. **Change data type** - Mudar tipo de dado de coluna existente
4. **Drop a column** - Remover coluna do schema
5. **Change column order** - Mudar ordem das colunas
6. **Intentional Schema Change** - Mudanças planejadas de schema
7. **Unintentional Schema Change** - Mudanças não planejadas de schema
8. **DMV versioning issue** - Investigação de problema de versionamento
