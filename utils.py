"""
Fonctions de traitement pour l'analyse FSP.
"""

import pandas as pd
import logging
import io
import re

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def load_fsp(file) -> pd.DataFrame:
    """
    Charge le fichier FSP à nous (export Salesforce brut ou nettoyé).
    Nettoyage automatique :
      - Détecte la ligne d'en-tête (contenant "Nom du compte")
      - Supprime les colonnes vides (colonne A et C du fichier brut)
      - Supprime les lignes footer (Total, copyright...)
      - Dédoublonne sur Nom du compte
      - Extrait le CODE PDV
    """
    # --- Étape 1 : lire le fichier brut (sans header) pour détecter la structure ---
    df_raw = pd.read_excel(file, header=None)
    logger.info(f"FSP brut chargé : {df_raw.shape[0]} lignes, {df_raw.shape[1]} colonnes")

    # --- Étape 2 : trouver la ligne d'en-tête (celle qui contient "Nom du compte") ---
    header_row = None
    for i in range(min(30, len(df_raw))):
        row_values = df_raw.iloc[i].astype(str).str.lower().tolist()
        if any("nom du compte" in v for v in row_values):
            header_row = i
            break

    if header_row is not None:
        # Fichier brut Salesforce : relire avec le bon header
        file.seek(0)
        df = pd.read_excel(file, header=header_row)
        logger.info(f"En-tête détecté à la ligne {header_row}")
    else:
        # Fichier déjà nettoyé : relire normalement
        file.seek(0)
        df = pd.read_excel(file)
        logger.info("Fichier FSP déjà nettoyé (en-tête en ligne 0)")

    # --- Étape 3 : supprimer colonnes vides et colonnes "Unnamed" parasites ---
    cols_before = len(df.columns)
    df = df.dropna(axis=1, how="all")
    unnamed_cols = [c for c in df.columns if str(c).startswith("Unnamed")]
    if unnamed_cols:
        df = df.drop(columns=unnamed_cols)
    if cols_before != len(df.columns):
        logger.info(f"Colonnes inutiles supprimées : {cols_before} → {len(df.columns)}")

    # --- Étape 4 : supprimer les lignes footer (Total, copyright, etc.) ---
    total_idx = None
    for col in df.columns:
        mask = df[col].astype(str).str.lower().str.strip() == "total"
        if mask.any():
            total_idx = mask.idxmax()
            break

    if total_idx is not None:
        before = len(df)
        df = df.loc[:total_idx - 1].copy()
        logger.info(f"Lignes footer supprimées à partir de la ligne {total_idx} : {before} → {len(df)}")

    # --- Étape 5 : trouver la colonne "Nom du compte" ---
    nom_col = _find_column(df, ["Nom du compte", "nom du compte", "NOM DU COMPTE"])
    if nom_col is None:
        for col in df.columns:
            sample = df[col].dropna().astype(str).head(20)
            if sample.str.match(r"^\d+\s*-\s*.+").sum() > len(sample) * 0.5:
                nom_col = col
                break

    if nom_col is None:
        raise ValueError(
            "Impossible de trouver la colonne 'Nom du compte' dans le fichier FSP. "
            "Le fichier doit contenir une colonne avec des valeurs au format 'XXXXX - NOM DU PDV'."
        )

    # --- Étape 6 : dédoublonnage ---
    before = len(df)
    df = df.drop_duplicates(subset=[nom_col]).copy()
    logger.info(f"FSP dédoublonné sur '{nom_col}' : {before} → {len(df)} lignes")

    # --- Étape 7 : extraire CODE PDV ---
    df["CODE_PDV"] = df[nom_col].astype(str).str.extract(r"^(\d+)")[0]
    df["CODE_PDV"] = pd.to_numeric(df["CODE_PDV"], errors="coerce").astype("Int64")
    logger.info(f"CODE_PDV extraits : {df['CODE_PDV'].notna().sum()} valides sur {len(df)}")
    return df


def load_christophe(file) -> pd.DataFrame:
    """Charge l'export Christophe (Excel ou CSV), dédoublonne sur NO_PDV."""
    filename = getattr(file, "name", "")

    if filename.lower().endswith(".csv"):
        content = file.read()
        file.seek(0)
        for enc in ["utf-8", "latin-1", "cp1252"]:
            for sep in [";", ",", "\t"]:
                try:
                    df = pd.read_csv(io.BytesIO(content), encoding=enc, sep=sep, nrows=5)
                    if len(df.columns) > 3:
                        df = pd.read_csv(io.BytesIO(content), encoding=enc, sep=sep)
                        logger.info(f"CSV chargé (enc={enc}, sep={repr(sep)}) : {df.shape[0]} lignes")
                        return _prepare_christophe(df)
                except Exception:
                    continue
        raise ValueError("Impossible de lire le fichier CSV. Vérifiez le format.")
    else:
        df = pd.read_excel(file)
        logger.info(f"Excel Christophe chargé : {df.shape[0]} lignes")
        return _prepare_christophe(df)


def _prepare_christophe(df: pd.DataFrame) -> pd.DataFrame:
    """Prépare Christophe : identifie colonnes, dédoublonne sur NO_PDV."""
    pdv_col = _find_column(df, ["NO_PDV", "no_pdv", "NO PDV", "code pdv", "CODE_PDV"])
    if pdv_col is None:
        raise ValueError(
            "Impossible de trouver la colonne 'NO_PDV' dans l'export Christophe. "
            "Le fichier doit contenir une colonne NO_PDV (code point de vente)."
        )

    df["NO_PDV"] = pd.to_numeric(df[pdv_col], errors="coerce").astype("Int64")

    # Trouver la colonne NB_T031T044 (paris — colonne K)
    bet_col = _find_column(df, ["NB_T031T044", "nb_t031t044", "NB T031T044"])
    if bet_col is not None:
        if df[bet_col].dtype == object:
            df["NB_T031T044"] = pd.to_numeric(
                df[bet_col].astype(str).str.replace(",", "."), errors="coerce"
            )
        else:
            df["NB_T031T044"] = pd.to_numeric(df[bet_col], errors="coerce")
    else:
        logger.warning("Colonne NB_T031T044 non trouvée — le filtre 'sans prise de paris' sera vide.")
        df["NB_T031T044"] = float("nan")

    # Dédoublonnage sur NO_PDV
    before = len(df)
    df = df.drop_duplicates(subset=["NO_PDV"]).copy()
    logger.info(f"Christophe dédoublonné sur NO_PDV : {before} → {len(df)} lignes")

    return df


def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Trouve une colonne parmi les candidats (insensible à la casse et aux espaces)."""
    col_map = {re.sub(r"\s+", " ", c.strip().lower()): c for c in df.columns}
    for candidate in candidates:
        normalized = re.sub(r"\s+", " ", candidate.strip().lower())
        if normalized in col_map:
            return col_map[normalized]
    return None


def compare_data(df_fsp: pd.DataFrame, df_chris: pd.DataFrame) -> pd.DataFrame:
    """
    Tableau 1 : CODE_PDV de FSP qui sont ABSENTS de la colonne NO_PDV de Christophe.
    """
    chris_pdv = set(df_chris["NO_PDV"].dropna().astype(int))
    mask = df_fsp["CODE_PDV"].apply(lambda x: pd.notna(x) and int(x) not in chris_pdv)
    result = df_fsp[mask].copy()
    logger.info(f"Tableau 1 — FSP non présent dans Christophe : {len(result)} lignes")
    return result


def filter_no_bets(df_fsp: pd.DataFrame, df_chris: pd.DataFrame) -> pd.DataFrame:
    """
    Tableau 2 :
    1) Prendre Christophe où NB_T031T044 est vide (colonne K)
    2) Ne garder que ceux dont NO_PDV est présent dans les CODE_PDV de FSP
    """
    # Étape 1 : lignes Christophe sans prise de paris
    chris_no_bets = df_chris[df_chris["NB_T031T044"].isna()].copy()
    logger.info(f"Christophe sans paris (colonne K vide) : {len(chris_no_bets)} lignes")

    # Étape 2 : ne garder que ceux dont NO_PDV est dans FSP
    fsp_codes = set(df_fsp["CODE_PDV"].dropna().astype(int))
    mask = chris_no_bets["NO_PDV"].apply(lambda x: pd.notna(x) and int(x) in fsp_codes)
    result = chris_no_bets[mask].copy()
    logger.info(f"Tableau 2 — Sans prise de paris ET dans FSP : {len(result)} lignes")
    return result


def merge_results(df_fsp: pd.DataFrame, df_diff: pd.DataFrame, df_no_bets: pd.DataFrame) -> pd.DataFrame:
    """
    Fusionne les 2 tableaux en un seul avec la colonne Nom du compte.
    - Tableau 1 (non présent Christophe) → Nom du compte directement depuis FSP
    - Tableau 2 (sans prise de paris) → retrouver Nom du compte via CODE_PDV/NO_PDV
    """
    # Trouver la colonne Nom du compte
    nom_col = None
    for c in df_fsp.columns:
        if "nom du compte" in c.lower():
            nom_col = c
            break

    rows = []

    # Tableau 1 : vient de FSP, on a directement Nom du compte
    if nom_col and len(df_diff) > 0:
        for _, r in df_diff.iterrows():
            rows.append({"Nom du compte": r[nom_col]})

    # Tableau 2 : vient de Christophe, on retrouve Nom du compte via FSP
    if len(df_no_bets) > 0:
        fsp_lookup = df_fsp.set_index("CODE_PDV")
        for _, r in df_no_bets.iterrows():
            code = r["NO_PDV"]
            if nom_col and code in fsp_lookup.index:
                nom = fsp_lookup.loc[code, nom_col]
                if isinstance(nom, pd.Series):
                    nom = nom.iloc[0]
            else:
                nom = str(code)
            rows.append({"Nom du compte": nom})

    result = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["Nom du compte"])
    logger.info(f"FSP restantes (fusion) : {len(result)} lignes")
    return result


def export_results(df_diff: pd.DataFrame, df_no_bets: pd.DataFrame, df_fsp: pd.DataFrame, df_merged: pd.DataFrame) -> bytes:
    """Génère un fichier Excel avec 3 onglets : différences, sans paris, FSP restantes (fusion)."""
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        cols_diff = [c for c in df_diff.columns if c != "CODE_PDV"]
        cols_bets = [c for c in df_no_bets.columns if c != "CODE_PDV"]

        df_diff[cols_diff].to_excel(writer, sheet_name="Non présent Christophe", index=False)
        df_no_bets[cols_bets].to_excel(writer, sheet_name="Sans prise de paris", index=False)
        df_merged.to_excel(writer, sheet_name="FSP restantes", index=False)

    return output.getvalue()
