from sqlalchemy import MetaData, Table, Column
from sqlalchemy.types import *

# TODO wollen wir hier immer die "neueste" Basis-Version nehmen?
#  (Ist in BASIS_VER_GUELTIGKEIT die version deren Beginn am kürzesten "zurück
#  liegt")

# oder spezif angabe?

# ================ KALENDER DATEN ================
# Was habe ich weggelassen? : Die "sprechenden Namen" von Betriebstagen und Tagesarten.
# Demzufolge auch weggelassen hab ich bspw. die Tabelle "MENGE_TAGESART", da die dann nur für die Namen relevant waere
# Heißt wenn bspw. der Foreign key in REC_FRT auf diese Tabelle verweist : erstmal nicht abgebildet. aber brauchen wir aus meiner sicht auch gar nicht.(?)

# Schema definieren
metadata = MetaData()
sqal_table_basis_ver_gueltigkeit = Table('BASIS_VER_GUELTIGKEIT', metadata,
              Column('ver_gueltigkeit', DECIMAL(8)),
              Column('basis_version', DECIMAL(9)))

# sqal_table_menge_basis_versionen = Table('MENGE_BASIS_VERSIONEN', metadata,
#               Column('basis_version', DECIMAL(9)),
#               Column('basis_version_text', String(40)))

#TODO das encoding für den betriebstag muss später irgendwo anders gemacht werden. sonst krieg ich die teile nicht rüber.
sqal_table_firmenkalender = Table('FIRMENKALENDER', metadata,
              Column('basis_version', DECIMAL(9)),
              Column('betriebstag', DECIMAL(8)),
              Column('tagesart_nr',  DECIMAL(3)))

# ================ ORTS DATEN ================

sqal_table_menge_onr_typ = Table('MENGE_ONR_TYP', metadata,
              Column('basis_version', DECIMAL(9)),
              Column('onr_typ_nr', DECIMAL(2))) # 1: Haltepunkt, 2: Betriebshofpunkt, 3: Ortsmarke, 4: LSA-Punkt, 5: Routenzwischenpunkt, 6: Betriebspunkt, 7: Grenzpunkt

sqal_table_menge_ort_typ = Table('MENGE_ORT_TYP', metadata,
              Column('basis_version', DECIMAL(9)),
              Column('ort_typ_nr', DECIMAL(2))) # 1: Haltestelle, 2: Betriebshof


# Haltepunkte,
sqal_table_rec_hp = Table('REC_HP', metadata,
              Column('basis_version', DECIMAL(9)),
              Column('ort_typ_nr', DECIMAL(2)),
              Column('ort_nr', DECIMAL(6)))

# Orte
# TODO Hier gibt es zwar das Attribute für Lat und Long, aber die sind optional, deswegen übernimm ich die erstmal nicht?
sqal_table_rec_ort = Table('REC_ORT', metadata,
              Column('basis_version', DECIMAL(9)),
              Column('ort_typ_nr', DECIMAL(2)),
              Column('ort_nr', DECIMAL(6)),
              Column('ort_name', String(40)))


# ================ FAHRZEUG- UND BETRIEBSDATEN ================
# bei Fahrzeug Tabelle: auch sowas wie Polizeiliches Kennzeichen und so hab ichj erst mal weg gelassen, wär aber vlt für die Namings später gut?
# bei Fahrzeug Typ Tabelle: Da ignorier ich so sachen wie bspw. Angaben zu Mengen an Sitzplätzen usw. (..)

sqal_table_rec_hp = Table('FAHRZEUG', metadata,
              Column('basis_version', DECIMAL(9)),
              Column('fzg_nr', DECIMAL(4)),
              Column('fzg_typ_nr', DECIMAL(6))) # TODO kann lt VDV 452 S. 39 aber auch NULL sein!

sqal_table_menge_bereich = Table('MENGE_BEREICH', metadata,
    Column('basis_version', DECIMAL(9)),
    Column('bereich_nr', DECIMAL(3))
)

sqal_table_menge_fzg_typ = Table('MENGE_FZG_TYP', metadata,
              Column('basis_version', DECIMAL(9)),
              Column('fzg_typ_nr', DECIMAL(6)),
              Column('fzg_laenge', DECIMAL(2)), # in Meter
              Column('fzg_typ_breite', DECIMAL(3)), # in cm!!
            Column('fzg_typ_gewicht', DECIMAL(6)),
            Column('fzg_typ_text', String(40)), # Bezeichner "Name" des Fahrzeugtyps
            Column('str_fzg_typ', String(6)), # Kurzname des Fahrzeugs
            Column('batterie_typ_nr', DECIMAL(4)),
            Column('verbrauch_dist_anz', DECIMAL(5)), # consumption, hier in Wh/km !
            Column('verbrauch_zeit', DECIMAL(5))) # Energieverbrauch pro h für Nebenverbraucher (Wh / h)


# ================ NETZ DATEN ================

# Verbindung A -> B mit Länge dieser Verbindung
sqal_table_rec_sel = Table('REC_SEL', metadata,
        Column('BASIS_VERSION', DECIMAL(9)),
          Column('BEREICH_NR', DECIMAL(9)),
          Column('ONR_TYP_NR', DECIMAL(2)),
          Column('ORT_NR', DECIMAL(6)),
          Column('SEL_ZIEL', DECIMAL(6)),
        Column('SEL_ZIEL_TYP', DECIMAL(2)),
        Column('SEL_LAENGE', String(5)), # Streckenlänge (knotenorientiert) in Meter
        )


# Fahrgruppen
sqal_table_menge_fgr = Table('MENGE_FGR', metadata,
      Column('BASIS_VERSION', DECIMAL(9)),
      Column('FGR_NR', DECIMAL(9)),
        )

# Haltezeit pro Fahrzeitgruppe und Ort
sqt_ort_hztf = Table('ORT_HZTF', metadata,
      Column('BASIS_VERSION', DECIMAL(9)),
      Column('FGR_NR', DECIMAL(9)),
    Column('ONR_TYP_NR', DECIMAL(2)),
    Column('ORT_NR', DECIMAL(6)),
     Column('HP_HZT', DECIMAL(6)),
)

sqt_sel_fzt_feld = Table('SEL_FZT_FELD', metadata,
    Column('BASIS_VERSION', DECIMAL(9)),
    Column('BEREICH_NR', DECIMAL(3)),
    Column('ONR_TYP_NR', DECIMAL(2)),
    Column('ORT_NR', DECIMAL(6)),
    Column('SEL_ZIEL', DECIMAL(6)),
    Column('SEL_ZIEL_TYP', DECIMAL(2)),
    Column('SEL_FZT', DECIMAL(6)), # Streckenfahrzeit je Fahrzeitgruppe in Sekunden
)


sqt_rec_ueb = Table('REC_UEB', metadata,
    Column('BASIS_VERSION', DECIMAL(9)),
    Column('BEREICH_NR', DECIMAL(3)),
    Column('ONR_TYP_NR', DECIMAL(2)),
    Column('ORT_NR', DECIMAL(6)),
    Column('UEB_ZIEL_TYP', DECIMAL(2)),
    Column('UEB_ZIEL', DECIMAL(6)),
    Column('UEB_LAENGE', DECIMAL(6))) # Länge des Überläuferfahrtwerges, in Meter

sqt_ueb_fzt = Table('UEB_FZT', metadata,
    Column('BASIS_VERSION', DECIMAL(9)),
    Column('BEREICH_NR', DECIMAL(3)),
    Column('FGR_NR', DECIMAL(9)),
    Column('ONR_TYP_NR', DECIMAL(2)),
    Column('ORT_NR', DECIMAL(6)),
    Column('UEB_ZIEL_TYP', DECIMAL(2)),
    Column('UEB_ZIEL', DECIMAL(6)),
    Column('UEB_FAHRZEIT', DECIMAL(6))) # Fahrzeit der Überlaeuferfahrt je Fahrzeitgruppe in Sekunden


# ================ LINIEN DATEN ================

sqt_lid_verlauf = Table('LID_VERLAUF', metadata,
                        Column('BASIS_VERSION', DECIMAL(9)),
                        Column('LI_LFD_NR', DECIMAL(3)),
                        Column('LI_NR', DECIMAL(6)),
                        Column('STR_LI_VAR', String(6)),
                        Column('ONR_TYP_NR', DECIMAL(2)),
                        Column('ORT_NR', DECIMAL(6)),
                        ) # TODO: hier gibt es das attribut PRODUKTIV eigentlich noch, aber es ist Optional, wenn es nicht gesetzt war, muss der wert aus FAHRTART_NR bezogen werden, hab es also erstmal weggelassen, zu klären.

# ROUTEN_ART könnte hier noch cool sein, war aber in v4.1 NICHT enthalten!
sqt_rec_lid = Table('REC_LID', metadata,
                    Column('BASIS_VERSION', DECIMAL(9)),
                    Column('BEREICH_NR', DECIMAL(3)),
                    Column('LI_NR', DECIMAL(6)),
                    Column('STR_LI_VAR', String(6)),
                    Column('LI_RI_NR', DECIMAL(3)),
                    Column('LI__KUERZEL', String(6)),
                    Column('LIDNAME', String(40))
                    )


# ================ FAHRPLAN DATEN ================

sqt_rec_frt = Table('REC_FRT', metadata,
        Column('BASIS_VERSION', DECIMAL(9)),
        Column('FRT_FID', DECIMAL(10)),
        Column('FRT_START', DECIMAL(6)),
        Column('LI_NR', DECIMAL(6)),
        Column('TAGESART_NR', DECIMAL(3)),
        Column('FGR_NR', DECIMAL(9)),
       Column('STR_LI_VAR', String(6)),
       Column('UM_UID', DECIMAL(8))
)

sqt_rec_frt_hzt = Table('REC_FRT_HZT', metadata,
        Column('BASIS_VERSION', DECIMAL(9)),
        Column('FRT_FID', DECIMAL(10)),
        Column('ONR_TYP_NR', DECIMAL(2)),
        Column('ORT_NR', DECIMAL(6)),
        Column('FRT_HZT_ZEIT', DECIMAL(6)) # Haltezeit in Sekunden am Haltepunkt.
)


sqt_rec_umlauf = Table('REC_UMLAUF', metadata,
        Column('BASIS_VERSION', DECIMAL(9)),
        Column('TAGESART_NR', DECIMAL(3)),
        Column('UM_UID', DECIMAL(8)),
       Column('ANF_ORT', DECIMAL(6)),
       Column('ANF_ONR_TYP', DECIMAL(2)),
       Column('END_ORT', DECIMAL(6)),
       Column('END_ONR_TYP', DECIMAL(2)),
       Column('FZG_TYP_NR', DECIMAL(3))
)