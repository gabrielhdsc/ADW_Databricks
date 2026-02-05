# Databricks notebook source

## 📊 Adventure Works – Profitability Dashboard (GO-LIVE)

 Documentação do dashboard de rentabilidade desenvolvido no Databricks SQL.

## 1. Visão Geral

 Este dashboard tem como objetivo analisar a rentabilidade das vendas, permitindo
 avaliar desempenho financeiro, identificar produtos deficitários e comparar
 margens de lucro por categoria e território.

## 2. Arquitetura de Dados

 - Camada consumida: Gold  
 - Tipo de objeto: Views SQL  
 - Atualização: Automática (baseada na Silver)  
 - Dashboard: Databricks SQL

## 3. Métricas Principais

 - **Revenue**: Receita total  
 - **Cost**: Custo total  
 - **Profit**: Revenue - Cost  
 - **Profit Margin**: Profit / Revenue

## 4. Visualizações do Dashboard

### 4.1 Revenue vs Profit – Trend Over Time  
**Fonte:** `vwgoliveprofitability`  

Mostra a evolução mês a mês da **receita** e do **lucro**, permitindo identificar tendências ao longo do tempo, quedas de desempenho e períodos em que o lucro foi reduzido ou negativo mesmo com aumento nas vendas.

---

### 4.2 Average Profit Margin by Territory  
**Fonte:** `vwgoliveprofitability`  

Compara a **margem média de lucro** entre territórios/países, evidenciando regiões onde o negócio é mais eficiente e aquelas que apresentam menor rentabilidade.

---

### 4.3 Revenue vs Cost – Top 10 Products (Last 2 Years)  
**Fonte:** `vwprofitabilityrevenuexcost`  

Compara **receita** e **custo** dos **10 produtos com maior volume de vendas nos últimos dois anos**, facilitando a identificação de produtos com boa folga de lucro e daqueles que operam com margem apertada ou prejuízo.

---

### 4.4 Average Profit Margin by Category  
 **Fonte:** `vwgoliveprofitability`  

Apresenta a **margem média de lucro por categoria de produto**, permitindo comparar categorias como *Accessories*, *Clothing* e *Bikes* e identificar quais são mais rentáveis proporcionalmente.

---

### 4.5 Products with Loss  
**Fonte:** `vwprofitabilitynotprofprod`  

Tabela detalhada que lista os **produtos com margem média negativa**, incluindo informações de país, custo, receita, lucro e margem. Essa visualização apoia análises detalhadas e ações corretivas direcionadas.

## 5. Observações Técnicas

 - O dashboard executa apenas SELECTs simples
 - Toda a lógica de negócio está centralizada nas views
 - O uso de views garante governança e atualização automática

## 6. Alertas

### 6.1 Alerta de Produtos com Margem Negativa (Ano Atual)

Este alerta monitora a rentabilidade dos produtos no **ano mais recente disponível**.

 - **Fonte de dados:** `vwgoliveprofitability`
 - **Regra de cálculo:** Margem = `SUM(Profit) / SUM(Revenue)`
 - **Escopo temporal:** Último ano disponível
 - **Condição do alerta:**  
   O alerta é disparado quando existe **ao menos um produto** com margem negativa.

 - **Critério técnico:**  
   `COUNT(ProductName) > 0`

 - **Objetivo:**  
   Identificar rapidamente produtos que estão gerando prejuízo, permitindo ações corretivas como revisão de custos, preços ou estratégia comercial