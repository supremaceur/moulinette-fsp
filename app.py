"""
Moulinette FSP - Application Streamlit
"""

import streamlit as st
import pandas as pd
from utils import load_fsp, load_christophe, compare_data, filter_no_bets, export_results

# --- Config & thème PMU ---
st.set_page_config(page_title="Moulinette FSP", page_icon="🏇", layout="wide")

PMU_CSS = """
<style>
    /* Palette PMU : vert foncé, or, blanc */
    :root {
        --pmu-green: #00643C;
        --pmu-green-light: #00834E;
        --pmu-gold: #C8A951;
        --pmu-gold-light: #E8D48B;
        --pmu-dark: #1A1A2E;
    }

    /* Header */
    .main-header {
        background: linear-gradient(135deg, var(--pmu-green) 0%, var(--pmu-green-light) 100%);
        padding: 1.5rem 2rem;
        border-radius: 12px;
        margin-bottom: 1.5rem;
        border-bottom: 4px solid var(--pmu-gold);
    }
    .main-header h1 {
        color: white !important;
        margin: 0 !important;
        font-size: 2rem !important;
    }
    .main-header p {
        color: var(--pmu-gold-light) !important;
        margin: 0.3rem 0 0 0 !important;
        font-size: 1rem;
    }

    /* Upload cards */
    .upload-card {
        background: linear-gradient(180deg, #f8f9fa 0%, #ffffff 100%);
        border: 2px solid #e0e0e0;
        border-radius: 12px;
        padding: 1.2rem;
        transition: border-color 0.2s;
    }
    .upload-card:hover {
        border-color: var(--pmu-gold);
    }
    .upload-label {
        color: var(--pmu-green);
        font-weight: 700;
        font-size: 1rem;
        margin-bottom: 0.5rem;
    }

    /* Metrics */
    [data-testid="stMetric"] {
        background: white;
        border: 1px solid #e8e8e8;
        border-radius: 10px;
        padding: 1rem;
        border-left: 4px solid var(--pmu-green);
    }
    [data-testid="stMetricLabel"] {
        color: #555 !important;
        font-size: 0.85rem !important;
    }
    [data-testid="stMetricValue"] {
        color: var(--pmu-green) !important;
        font-weight: 700 !important;
    }

    /* Buttons */
    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, var(--pmu-green) 0%, var(--pmu-green-light) 100%) !important;
        border: none !important;
        color: white !important;
        font-weight: 600 !important;
        padding: 0.6rem 2rem !important;
        border-radius: 8px !important;
        font-size: 1rem !important;
    }
    .stButton > button[kind="primary"]:hover {
        background: linear-gradient(135deg, var(--pmu-green-light) 0%, var(--pmu-green) 100%) !important;
        border: none !important;
    }
    .stDownloadButton > button {
        background: linear-gradient(135deg, var(--pmu-gold) 0%, #B8993D 100%) !important;
        border: none !important;
        color: white !important;
        font-weight: 600 !important;
        padding: 0.6rem 2rem !important;
        border-radius: 8px !important;
    }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0;
        border-bottom: 2px solid #e0e0e0;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 0.7rem 1.5rem;
        font-weight: 600;
        color: #666;
    }
    .stTabs [aria-selected="true"] {
        color: var(--pmu-green) !important;
        border-bottom: 3px solid var(--pmu-green) !important;
    }

    /* Section titles */
    .section-title {
        color: var(--pmu-green);
        font-weight: 700;
        font-size: 1.2rem;
        border-left: 4px solid var(--pmu-gold);
        padding-left: 0.8rem;
        margin: 1rem 0;
    }

    /* Info banner */
    .info-banner {
        background: linear-gradient(135deg, #f0f7f4 0%, #e8f5e9 100%);
        border: 1px solid var(--pmu-green-light);
        border-radius: 8px;
        padding: 1rem;
        color: var(--pmu-green);
        text-align: center;
        font-weight: 500;
    }

    /* Dataframe */
    .stDataFrame {
        border-radius: 8px;
        overflow: hidden;
    }
</style>
"""

st.markdown(PMU_CSS, unsafe_allow_html=True)

# --- Header ---
st.markdown("""
<div class="main-header">
    <h1>Moulinette FSP</h1>
    <p>Tri et filtrage des FSP</p>
</div>
""", unsafe_allow_html=True)

# --- Upload ---
col1, col2 = st.columns(2)

with col1:
    st.markdown('<div class="upload-card"><div class="upload-label">📂 Fichier FSP à nous</div></div>', unsafe_allow_html=True)
    fsp_file = st.file_uploader(
        "Importer le fichier FSP (Excel)",
        type=["xlsx", "xls"],
        key="fsp",
        label_visibility="collapsed",
    )

with col2:
    st.markdown('<div class="upload-card"><div class="upload-label">📂 Export Christophe</div></div>', unsafe_allow_html=True)
    chris_file = st.file_uploader(
        "Importer l'export Christophe (Excel ou CSV)",
        type=["xlsx", "xls", "csv"],
        key="chris",
        label_visibility="collapsed",
    )

# --- Aperçu fichiers chargés ---
if fsp_file or chris_file:
    st.markdown("")
    prev1, prev2 = st.columns(2)
    if fsp_file:
        with prev1:
            try:
                df_preview_fsp = pd.read_excel(fsp_file)
                fsp_file.seek(0)
                st.caption(f"✅ FSP : {len(df_preview_fsp)} lignes · {len(df_preview_fsp.columns)} colonnes")
                st.dataframe(df_preview_fsp.head(5), use_container_width=True, height=200)
            except Exception as e:
                st.error(f"Erreur lecture FSP : {e}")
    if chris_file:
        with prev2:
            try:
                name = chris_file.name.lower()
                if name.endswith(".csv"):
                    import io
                    content = chris_file.read()
                    chris_file.seek(0)
                    for enc in ["utf-8", "latin-1", "cp1252"]:
                        for sep in [";", ",", "\t"]:
                            try:
                                df_prev = pd.read_csv(io.BytesIO(content), encoding=enc, sep=sep, nrows=5)
                                if len(df_prev.columns) > 3:
                                    df_prev_full = pd.read_csv(io.BytesIO(content), encoding=enc, sep=sep)
                                    st.caption(f"✅ Christophe : {len(df_prev_full)} lignes · {len(df_prev_full.columns)} colonnes")
                                    st.dataframe(df_prev_full.head(5), use_container_width=True, height=200)
                                    raise StopIteration
                            except StopIteration:
                                raise
                            except Exception:
                                continue
                else:
                    df_preview_chris = pd.read_excel(chris_file)
                    chris_file.seek(0)
                    st.caption(f"✅ Christophe : {len(df_preview_chris)} lignes · {len(df_preview_chris.columns)} colonnes")
                    st.dataframe(df_preview_chris.head(5), use_container_width=True, height=200)
            except StopIteration:
                pass
            except Exception as e:
                st.error(f"Erreur lecture Christophe : {e}")

# --- Lancement analyse ---
st.markdown("")

if st.button("🚀  Lancer l'analyse", type="primary", disabled=not (fsp_file and chris_file), use_container_width=True):
    if not fsp_file or not chris_file:
        st.warning("Veuillez importer les 2 fichiers.")
    else:
        try:
            with st.spinner("Chargement des données..."):
                df_fsp = load_fsp(fsp_file)
                df_chris = load_christophe(chris_file)

            with st.spinner("Analyse en cours..."):
                df_diff = compare_data(df_fsp, df_chris)
                df_no_bets = filter_no_bets(df_fsp, df_chris)

            # Fusion des 2 tableaux (Nom du compte uniquement)
            from utils import merge_results
            df_merged = merge_results(df_fsp, df_diff, df_no_bets)
            nb_merged = len(df_merged)

            # --- Statistiques ---
            st.markdown("")
            st.markdown('<div class="section-title">Résumé</div>', unsafe_allow_html=True)

            nb_fsp = df_fsp["CODE_PDV"].notna().sum()

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("FSP du jour", nb_fsp)
            m2.metric("Non présent Christophe", len(df_diff))
            m3.metric("Sans prise de paris", len(df_no_bets))
            m4.metric("FSP restantes", nb_merged)

            # --- Tableaux résultats en onglets ---
            st.markdown("")
            st.markdown('<div class="section-title">Résultats</div>', unsafe_allow_html=True)

            tab1, tab2, tab3 = st.tabs([
                f"Non présent Christophe ({len(df_diff)})",
                f"Sans prise de paris ({len(df_no_bets)})",
                f"FSP restantes ({nb_merged})",
            ])

            with tab1:
                if len(df_diff) > 0:
                    display_cols = [c for c in df_diff.columns if c != "CODE_PDV"]
                    st.dataframe(df_diff[display_cols].reset_index(drop=True), use_container_width=True)
                else:
                    st.success("Tous les PDV sont présents dans l'export Christophe.")

            with tab2:
                if len(df_no_bets) > 0:
                    display_cols = [c for c in df_no_bets.columns if c != "CODE_PDV"]
                    st.dataframe(df_no_bets[display_cols].reset_index(drop=True), use_container_width=True)
                else:
                    st.success("Tous les PDV présents ont des prises de paris.")

            with tab3:
                if nb_merged > 0:
                    st.dataframe(df_merged, use_container_width=True)
                else:
                    st.info("Aucun PDV à afficher.")

            # --- Export ---
            st.markdown("")
            excel_data = export_results(df_diff, df_no_bets, df_fsp, df_merged)
            st.download_button(
                label="📥  Télécharger le résultat en Excel",
                data=excel_data,
                file_name="resultat_fsp.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        except Exception as e:
            st.error(f"Erreur lors de l'analyse : {e}")
            st.exception(e)

elif not fsp_file or not chris_file:
    st.markdown('<div class="info-banner">Importez les 2 fichiers puis cliquez sur <strong>Lancer l\'analyse</strong></div>', unsafe_allow_html=True)

# --- Footer confidentialité ---
st.markdown("""
<div style="text-align:center; margin-top:3rem; padding:1rem; border-top:1px solid #333; color:#888; font-size:0.8rem;">
    🔒 Aucune donnée n'est conservée. Les fichiers sont traités en mémoire et supprimés dès la fin de la session.
</div>
""", unsafe_allow_html=True)
