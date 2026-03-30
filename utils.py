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
    """Charge le fichier FSP à nous, dédoublonne sur Nom du compte, extrait le CODE PDV (5 chiffres)."""
    df = pd.read_excel(file)
    logger.info(f"FSP chargé : {df.shape[0]} lignes, {df.shape[1]} colonnes")

    # Trouver la colonne "Nom du compte"
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

    # Dédoublonnage sur "Nom du compte"
    before = len(df)
    df = df.drop_duplicates(subset=[nom_col]).copy()
    logger.info(f"FSP dédoublonné sur '{nom_col}' : {before} → {len(df)} lignes")

    # Extraire le code 5 chiffres (XXXXX) depuis "XXXXX - NOM"
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


def export_results(df_diff: pd.DataFrame, df_no_bets: pd.DataFrame, df_fsp: pd.DataFrame) -> bytes:
    """Génère un fichier Excel avec 3 onglets : différences, sans paris, synthèse."""
    output = io.BytesIO()

    # Trouver la colonne Nom du compte dans FSP
    nom_col = None
    for c in df_fsp.columns:
        if "nom du compte" in c.lower():
            nom_col = c
            break

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        cols_diff = [c for c in df_diff.columns if c != "CODE_PDV"]
        cols_bets = [c for c in df_no_bets.columns if c != "CODE_PDV"]

        df_diff[cols_diff].to_excel(writer, sheet_name="Non présent Christophe", index=False)
        df_no_bets[cols_bets].to_excel(writer, sheet_name="Sans prise de paris", index=False)

        # Onglet Synthèse : fusion des 2 tableaux avec juste Nom du compte
        rows = []
        if nom_col and len(df_diff) > 0:
            for _, r in df_diff.iterrows():
                rows.append({"Nom du compte": r[nom_col], "Catégorie": "Non présent Christophe"})
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
                rows.append({"Nom du compte": nom, "Catégorie": "Sans prise de paris"})

        if rows:
            pd.DataFrame(rows).to_excel(writer, sheet_name="Synthèse", index=False)

    return output.getvalue()
