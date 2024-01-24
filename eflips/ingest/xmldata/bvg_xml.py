from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import List, Optional

__NAMESPACE__ = "http://www.ivu.de/mb/intf/passengercount/remote/model/"


@dataclass
class AnzeigeText:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: str = field(
        default="",
        metadata={
            "required": True,
        },
    )


@dataclass
class Auftraggeber:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(default=None)


@dataclass
class Beginn:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(
        default=None,
        metadata={
            "required": True,
        },
    )


@dataclass
class Betriebshof:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(default=None)


@dataclass
class Build:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: str = field(default="")


@dataclass
class Datenversion:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    deployment_id: Optional[str] = field(
        default=None,
        metadata={
            "name": "deploymentID",
            "type": "Element",
            "required": True,
        },
    )


@dataclass
class Ende:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(
        default=None,
        metadata={
            "required": True,
        },
    )


@dataclass
class Endpunkt:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(
        default=None,
        metadata={
            "required": True,
        },
    )


@dataclass
class EntfernungVomStart:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(default=None)


class FahrgastwechselValue(Enum):
    J = "J"
    N = "N"


@dataclass
class Fahrplanbuchname:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: str = field(
        default="",
        metadata={
            "required": True,
        },
    )


@dataclass
class FahrtId:
    class Meta:
        name = "FahrtID"
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(
        default=None,
        metadata={
            "required": True,
        },
    )


@dataclass
class Fahrtart:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: str = field(default="")


@dataclass
class FahrzeitprofilNummer:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(
        default=None,
        metadata={
            "required": True,
        },
    )


@dataclass
class Fahrzeugtyp:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: str = field(default="")


@dataclass
class Fremdunternehmer:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(default=None)


@dataclass
class Gebietskoerperschaft:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    content: List[object] = field(
        default_factory=list,
        metadata={
            "type": "Wildcard",
            "namespace": "##any",
            "mixed": True,
            "choices": (
                {
                    "name": "Kurzname",
                    "type": str,
                },
                {
                    "name": "Langname",
                    "type": str,
                },
            ),
        },
    )


@dataclass
class Haltestellenbereich:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    content: List[object] = field(
        default_factory=list,
        metadata={
            "type": "Wildcard",
            "namespace": "##any",
            "mixed": True,
            "choices": (
                {
                    "name": "Nummer",
                    "type": int,
                },
                {
                    "name": "Kurzname",
                    "type": str,
                },
                {
                    "name": "Fahrplanbuchname",
                    "type": str,
                },
            ),
        },
    )


class HauptrouteValue(Enum):
    J = "J"
    N = "N"


@dataclass
class Id:
    class Meta:
        name = "ID"
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(
        default=None,
        metadata={
            "required": True,
        },
    )


@dataclass
class Kalenderdatum:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: str = field(default="")


@dataclass
class Kalendertag:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: str = field(
        default="",
        metadata={
            "required": True,
        },
    )


@dataclass
class Kalenderzeitraum:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    kalendertag: List[str] = field(
        default_factory=list,
        metadata={
            "name": "Kalendertag",
            "type": "Element",
            "min_occurs": 1,
        },
    )


@dataclass
class Kurzname:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: str = field(
        default="",
        metadata={
            "required": True,
        },
    )


@dataclass
class Langname:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: str = field(
        default="",
        metadata={
            "required": True,
        },
    )


@dataclass
class LfdNr:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(
        default=None,
        metadata={
            "required": True,
        },
    )


@dataclass
class LfdNrRoute:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(
        default=None,
        metadata={
            "required": True,
        },
    )


@dataclass
class LfdNrRoutenvariante:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(
        default=None,
        metadata={
            "required": True,
        },
    )


class MeldungskategorieValue(Enum):
    VALUE_0 = 0
    VALUE_2 = 2


class MeldungsnummerValue(Enum):
    VALUE_0 = 0
    VALUE_18 = 18
    VALUE_5 = 5


class MeldungstextValue(Enum):
    ANFRAGE_WURDE_KORREKT_ABGEARBEITET = "Anfrage wurde korrekt abgearbeitet."
    KEINE_UML_UFE_VORHANDEN = "Keine Umläufe vorhanden."
    KEINE_G_LTIGE_LINIE = "Keine gültige Linie."


class NameValue(Enum):
    BETRIEBSBEREICH = "Betriebsbereich"
    KEINE_ANZEIGEDATEN = "KeineAnzeigedaten"
    KEINE_GUELTIGKEITEN = "KeineGueltigkeiten"
    LINIE = "Linie"
    MELDUNGSKATEGORIE = "Meldungskategorie"
    STICHTAG = "Stichtag"


class NetzpunkttypValue(Enum):
    APKT = "APkt"
    BPUNKT = "BPunkt"
    EPKT = "EPkt"
    GPKT = "GPkt"
    HST = "Hst"


@dataclass
class Nummer:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(
        default=None,
        metadata={
            "required": True,
        },
    )


class ReleaseValue(Enum):
    VALUE_21_2_0_20220203_0833_898844 = "21.2.0-20220203-0833-898844"


class ReturnCodeValue(Enum):
    VALUE_0 = 0
    VALUE_1 = 1


class RichtungValue(Enum):
    VALUE_0 = 0
    VALUE_1 = 1
    VALUE_2 = 2


@dataclass
class Startpunkt:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(
        default=None,
        metadata={
            "required": True,
        },
    )


@dataclass
class Startzeit:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(
        default=None,
        metadata={
            "required": True,
        },
    )


@dataclass
class Startzeitpunkt:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: str = field(
        default="",
        metadata={
            "required": True,
        },
    )


@dataclass
class StreckenId:
    class Meta:
        name = "StreckenID"
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(
        default=None,
        metadata={
            "required": True,
        },
    )


@dataclass
class Streckenfahrzeit:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(
        default=None,
        metadata={
            "required": True,
        },
    )


@dataclass
class Streckenlaenge:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(
        default=None,
        metadata={
            "required": True,
        },
    )


@dataclass
class UmlaufId:
    class Meta:
        name = "UmlaufID"
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(
        default=None,
        metadata={
            "required": True,
        },
    )


@dataclass
class Umlaufbezeichnung:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: str = field(
        default="",
        metadata={
            "required": True,
        },
    )


@dataclass
class Version:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[Decimal] = field(default=None)


@dataclass
class Wagenfolgenummer:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(
        default=None,
        metadata={
            "required": True,
        },
    )


@dataclass
class Wartezeit:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(default=None)


@dataclass
class Wert:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: str = field(
        default="",
        metadata={
            "required": True,
        },
    )


@dataclass
class Xkoordinate:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(
        default=None,
        metadata={
            "required": True,
        },
    )


@dataclass
class Ykoordinate:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(
        default=None,
        metadata={
            "required": True,
        },
    )


@dataclass
class Zielanzeige:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    content: List[object] = field(
        default_factory=list,
        metadata={
            "type": "Wildcard",
            "namespace": "##any",
            "mixed": True,
            "choices": (
                {
                    "name": "Nummer",
                    "type": int,
                },
                {
                    "name": "AnzeigeText",
                    "type": str,
                },
            ),
        },
    )


@dataclass
class DeploymentId:
    class Meta:
        name = "deploymentID"
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: str = field(default="")


@dataclass
class ExterneRoutennummer:
    class Meta:
        name = "externeRoutennummer"
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[int] = field(
        default=None,
        metadata={
            "required": True,
        },
    )


class FahrgastrelevantValue(Enum):
    J = "J"
    N = "N"


class MitFahrgastwechselValue(Enum):
    J = "J"
    N = "N"


class VeroeffentlichtValue(Enum):
    J = "J"
    N = "N"


@dataclass
class ZugeordneteBetriebshoefe:
    class Meta:
        name = "zugeordneteBetriebshoefe"
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    betriebshof: List[int] = field(
        default_factory=list,
        metadata={
            "name": "Betriebshof",
            "type": "Element",
        },
    )


@dataclass
class Fahrgastwechsel:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[FahrgastwechselValue] = field(default=None)


@dataclass
class Gebietskoerperschaften:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    gebietskoerperschaft: List[Gebietskoerperschaft] = field(
        default_factory=list,
        metadata={
            "name": "Gebietskoerperschaft",
            "type": "Element",
        },
    )


@dataclass
class Gueltigkeiten:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    kalenderzeitraum: Optional[Kalenderzeitraum] = field(
        default=None,
        metadata={
            "name": "Kalenderzeitraum",
            "type": "Element",
            "required": True,
        },
    )


@dataclass
class Haltestellenbereiche:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    haltestellenbereich: List[Haltestellenbereich] = field(
        default_factory=list,
        metadata={
            "name": "Haltestellenbereich",
            "type": "Element",
        },
    )


@dataclass
class Hauptroute:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[HauptrouteValue] = field(default=None)


@dataclass
class Meldung:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    meldungskategorie: Optional[MeldungskategorieValue] = field(
        default=None,
        metadata={
            "name": "Meldungskategorie",
            "type": "Element",
            "required": True,
        },
    )
    meldungsnummer: Optional[MeldungsnummerValue] = field(
        default=None,
        metadata={
            "name": "Meldungsnummer",
            "type": "Element",
            "required": True,
        },
    )
    meldungstext: Optional[MeldungstextValue] = field(
        default=None,
        metadata={
            "name": "Meldungstext",
            "type": "Element",
            "required": True,
        },
    )


@dataclass
class Meldungskategorie:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[MeldungskategorieValue] = field(default=None)


@dataclass
class Meldungsnummer:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[MeldungsnummerValue] = field(default=None)


@dataclass
class Meldungstext:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[MeldungstextValue] = field(default=None)


@dataclass
class Name:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[NameValue] = field(default=None)


@dataclass
class Netzpunkttyp:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[NetzpunkttypValue] = field(default=None)


@dataclass
class Parameter:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    name: Optional[NameValue] = field(
        default=None,
        metadata={
            "name": "Name",
            "type": "Element",
            "required": True,
        },
    )
    wert: Optional[str] = field(
        default=None,
        metadata={
            "name": "Wert",
            "type": "Element",
            "required": True,
        },
    )


@dataclass
class Release:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[ReleaseValue] = field(default=None)


@dataclass
class ReturnCode:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[ReturnCodeValue] = field(default=None)


@dataclass
class Richtung:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[RichtungValue] = field(default=None)


@dataclass
class Schnittstellenversion:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    version: Optional[Decimal] = field(
        default=None,
        metadata={
            "name": "Version",
            "type": "Element",
            "required": True,
        },
    )
    release: Optional[ReleaseValue] = field(
        default=None,
        metadata={
            "name": "Release",
            "type": "Element",
            "required": True,
        },
    )
    build: Optional[str] = field(
        default=None,
        metadata={
            "name": "Build",
            "type": "Element",
            "required": True,
        },
    )


@dataclass
class Zielanzeigen:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    zielanzeige: List[Zielanzeige] = field(
        default_factory=list,
        metadata={
            "name": "Zielanzeige",
            "type": "Element",
        },
    )


@dataclass
class Fahrgastrelevant:
    class Meta:
        name = "fahrgastrelevant"
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[FahrgastrelevantValue] = field(default=None)


@dataclass
class MitFahrgastwechsel:
    class Meta:
        name = "mitFahrgastwechsel"
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[MitFahrgastwechselValue] = field(default=None)


@dataclass
class Veroeffentlicht:
    class Meta:
        name = "veroeffentlicht"
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    value: Optional[VeroeffentlichtValue] = field(default=None)


@dataclass
class GenerierungsParameter:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    parameter: List[Parameter] = field(
        default_factory=list,
        metadata={
            "name": "Parameter",
            "type": "Element",
            "min_occurs": 1,
        },
    )


@dataclass
class Meldungsliste:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    meldung: Optional[Meldung] = field(
        default=None,
        metadata={
            "name": "Meldung",
            "type": "Element",
            "required": True,
        },
    )


@dataclass
class Netzpunkt:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    content: List[object] = field(
        default_factory=list,
        metadata={
            "type": "Wildcard",
            "namespace": "##any",
            "mixed": True,
            "choices": (
                {
                    "name": "Nummer",
                    "type": int,
                },
                {
                    "name": "Kurzname",
                    "type": str,
                },
                {
                    "name": "Langname",
                    "type": str,
                },
                {
                    "name": "Netzpunkttyp",
                    "type": NetzpunkttypValue,
                },
                {
                    "name": "Xkoordinate",
                    "type": int,
                },
                {
                    "name": "Ykoordinate",
                    "type": int,
                },
                {
                    "name": "Haltestellenbereich",
                    "type": Haltestellenbereich,
                },
                {
                    "name": "mitFahrgastwechsel",
                    "type": MitFahrgastwechselValue,
                },
                {
                    "name": "Gebietskoerperschaften",
                    "type": Gebietskoerperschaften,
                },
            ),
        },
    )


@dataclass
class Ergebnis:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    return_code: Optional[ReturnCodeValue] = field(
        default=None,
        metadata={
            "name": "ReturnCode",
            "type": "Element",
            "required": True,
        },
    )
    meldungsliste: Optional[Meldungsliste] = field(
        default=None,
        metadata={
            "name": "Meldungsliste",
            "type": "Element",
            "required": True,
        },
    )


@dataclass
class Netzpunkte:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    netzpunkt: List[Netzpunkt] = field(
        default_factory=list,
        metadata={
            "name": "Netzpunkt",
            "type": "Element",
        },
    )


@dataclass
class Punkt:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    lfd_nr: Optional[int] = field(
        default=None,
        metadata={
            "name": "LfdNr",
            "type": "Element",
            "required": True,
        },
    )
    netzpunkt: Optional[Netzpunkt] = field(
        default=None,
        metadata={
            "name": "Netzpunkt",
            "type": "Element",
        },
    )
    fahrgastwechsel: Optional[FahrgastwechselValue] = field(
        default=None,
        metadata={
            "name": "Fahrgastwechsel",
            "type": "Element",
        },
    )
    veroeffentlicht: List[VeroeffentlichtValue] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "max_occurs": 2,
        },
    )
    zielanzeige: Optional[Zielanzeige] = field(
        default=None,
        metadata={
            "name": "Zielanzeige",
            "type": "Element",
        },
    )
    streckenfahrzeit: Optional[int] = field(
        default=None,
        metadata={
            "name": "Streckenfahrzeit",
            "type": "Element",
        },
    )
    wartezeit: Optional[int] = field(
        default=None,
        metadata={
            "name": "Wartezeit",
            "type": "Element",
        },
    )


@dataclass
class Zwischenpunkt:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    netzpunkt: Optional[Netzpunkt] = field(
        default=None,
        metadata={
            "name": "Netzpunkt",
            "type": "Element",
            "required": True,
        },
    )
    entfernung_vom_start: Optional[int] = field(
        default=None,
        metadata={
            "name": "EntfernungVomStart",
            "type": "Element",
            "required": True,
        },
    )


@dataclass
class Fahrzeitprofilpunkte:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    punkt: List[Punkt] = field(
        default_factory=list,
        metadata={
            "name": "Punkt",
            "type": "Element",
            "min_occurs": 1,
        },
    )


@dataclass
class Generierung:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    startzeitpunkt: Optional[str] = field(
        default=None,
        metadata={
            "name": "Startzeitpunkt",
            "type": "Element",
            "required": True,
        },
    )
    schnittstellenversion: Optional[Schnittstellenversion] = field(
        default=None,
        metadata={
            "name": "Schnittstellenversion",
            "type": "Element",
            "required": True,
        },
    )
    datenversion: Optional[Datenversion] = field(
        default=None,
        metadata={
            "name": "Datenversion",
            "type": "Element",
            "required": True,
        },
    )
    ergebnis: Optional[Ergebnis] = field(
        default=None,
        metadata={
            "name": "Ergebnis",
            "type": "Element",
            "required": True,
        },
    )
    generierungs_parameter: Optional[GenerierungsParameter] = field(
        default=None,
        metadata={
            "name": "GenerierungsParameter",
            "type": "Element",
            "required": True,
        },
    )


@dataclass
class Punktfolge:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    punkt: List[Punkt] = field(
        default_factory=list,
        metadata={
            "name": "Punkt",
            "type": "Element",
            "min_occurs": 1,
        },
    )


@dataclass
class Zwischenpunkte:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    zwischenpunkt: Optional[Zwischenpunkt] = field(
        default=None,
        metadata={
            "name": "Zwischenpunkt",
            "type": "Element",
        },
    )


@dataclass
class AbweichendeVeroeffentlichung:
    class Meta:
        name = "abweichendeVeroeffentlichung"
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    punkt: List[Punkt] = field(
        default_factory=list,
        metadata={
            "name": "Punkt",
            "type": "Element",
            "min_occurs": 1,
        },
    )


@dataclass
class Fahrzeitprofil:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    content: List[object] = field(
        default_factory=list,
        metadata={
            "type": "Wildcard",
            "namespace": "##any",
            "mixed": True,
            "choices": (
                {
                    "name": "FahrzeitprofilNummer",
                    "type": int,
                },
                {
                    "name": "Fahrzeitprofilpunkte",
                    "type": Fahrzeitprofilpunkte,
                },
            ),
        },
    )


@dataclass
class Strecke:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    id: Optional[int] = field(
        default=None,
        metadata={
            "name": "ID",
            "type": "Element",
        },
    )
    startpunkt: Optional[int] = field(
        default=None,
        metadata={
            "name": "Startpunkt",
            "type": "Element",
        },
    )
    endpunkt: Optional[int] = field(
        default=None,
        metadata={
            "name": "Endpunkt",
            "type": "Element",
        },
    )
    streckenlaenge: Optional[int] = field(
        default=None,
        metadata={
            "name": "Streckenlaenge",
            "type": "Element",
        },
    )
    zwischenpunkte: Optional[Zwischenpunkte] = field(
        default=None,
        metadata={
            "name": "Zwischenpunkte",
            "type": "Element",
        },
    )
    lfd_nr: Optional[int] = field(
        default=None,
        metadata={
            "name": "LfdNr",
            "type": "Element",
        },
    )
    strecken_id: Optional[int] = field(
        default=None,
        metadata={
            "name": "StreckenID",
            "type": "Element",
        },
    )
    auftraggeber: List[int] = field(
        default_factory=list,
        metadata={
            "name": "Auftraggeber",
            "type": "Element",
            "min_occurs": 1,
            "max_occurs": 2,
            "sequence": 1,
        },
    )


@dataclass
class Fahrzeitprofile:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    fahrzeitprofil: List[Fahrzeitprofil] = field(
        default_factory=list,
        metadata={
            "name": "Fahrzeitprofil",
            "type": "Element",
            "min_occurs": 1,
        },
    )


@dataclass
class Strecken:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    strecke: List[Strecke] = field(
        default_factory=list,
        metadata={
            "name": "Strecke",
            "type": "Element",
        },
    )


@dataclass
class Streckenfolge:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    strecke: List[Strecke] = field(
        default_factory=list,
        metadata={
            "name": "Strecke",
            "type": "Element",
            "min_occurs": 1,
        },
    )


@dataclass
class AbweichenderAuftraggeber:
    class Meta:
        name = "abweichenderAuftraggeber"
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    strecke: List[Strecke] = field(
        default_factory=list,
        metadata={
            "name": "Strecke",
            "type": "Element",
            "min_occurs": 1,
        },
    )


@dataclass
class Route:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    lfd_nr: Optional[int] = field(
        default=None,
        metadata={
            "name": "LfdNr",
            "type": "Element",
            "required": True,
        },
    )
    externe_routennummer: Optional[int] = field(
        default=None,
        metadata={
            "name": "externeRoutennummer",
            "type": "Element",
            "required": True,
        },
    )
    richtung: Optional[RichtungValue] = field(
        default=None,
        metadata={
            "name": "Richtung",
            "type": "Element",
            "required": True,
        },
    )
    hauptroute: Optional[HauptrouteValue] = field(
        default=None,
        metadata={
            "name": "Hauptroute",
            "type": "Element",
            "required": True,
        },
    )
    zielanzeigen: Optional[Zielanzeigen] = field(
        default=None,
        metadata={
            "name": "Zielanzeigen",
            "type": "Element",
            "required": True,
        },
    )
    streckenfolge: Optional[Streckenfolge] = field(
        default=None,
        metadata={
            "name": "Streckenfolge",
            "type": "Element",
            "required": True,
        },
    )
    punktfolge: Optional[Punktfolge] = field(
        default=None,
        metadata={
            "name": "Punktfolge",
            "type": "Element",
            "required": True,
        },
    )
    fahrzeitprofile: Optional[Fahrzeitprofile] = field(
        default=None,
        metadata={
            "name": "Fahrzeitprofile",
            "type": "Element",
            "required": True,
        },
    )


@dataclass
class Routenvariante:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    lfd_nr: Optional[int] = field(
        default=None,
        metadata={
            "name": "LfdNr",
            "type": "Element",
            "required": True,
        },
    )
    lfd_nr_route: Optional[int] = field(
        default=None,
        metadata={
            "name": "LfdNrRoute",
            "type": "Element",
            "required": True,
        },
    )
    abweichender_auftraggeber: Optional[AbweichenderAuftraggeber] = field(
        default=None,
        metadata={
            "name": "abweichenderAuftraggeber",
            "type": "Element",
        },
    )
    abweichende_veroeffentlichung: Optional[AbweichendeVeroeffentlichung] = field(
        default=None,
        metadata={
            "name": "abweichendeVeroeffentlichung",
            "type": "Element",
        },
    )


@dataclass
class StreckennetzDaten:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    haltestellenbereiche: Optional[Haltestellenbereiche] = field(
        default=None,
        metadata={
            "name": "Haltestellenbereiche",
            "type": "Element",
            "required": True,
        },
    )
    netzpunkte: Optional[Netzpunkte] = field(
        default=None,
        metadata={
            "name": "Netzpunkte",
            "type": "Element",
            "required": True,
        },
    )
    gebietskoerperschaften: Optional[Gebietskoerperschaften] = field(
        default=None,
        metadata={
            "name": "Gebietskoerperschaften",
            "type": "Element",
            "required": True,
        },
    )
    strecken: Optional[Strecken] = field(
        default=None,
        metadata={
            "name": "Strecken",
            "type": "Element",
            "required": True,
        },
    )


@dataclass
class RoutenDaten:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    route: List[Route] = field(
        default_factory=list,
        metadata={
            "name": "Route",
            "type": "Element",
        },
    )


@dataclass
class Routenvarianten:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    routenvariante: List[Routenvariante] = field(
        default_factory=list,
        metadata={
            "name": "Routenvariante",
            "type": "Element",
        },
    )


@dataclass
class Linie:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    content: List[object] = field(
        default_factory=list,
        metadata={
            "type": "Wildcard",
            "namespace": "##any",
            "mixed": True,
            "choices": (
                {
                    "name": "Kurzname",
                    "type": str,
                },
                {
                    "name": "zugeordneteBetriebshoefe",
                    "type": ZugeordneteBetriebshoefe,
                },
                {
                    "name": "RoutenDaten",
                    "type": RoutenDaten,
                },
                {
                    "name": "Routenvarianten",
                    "type": Routenvarianten,
                },
            ),
        },
    )


@dataclass
class Fahrt:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    id: Optional[int] = field(
        default=None,
        metadata={
            "name": "ID",
            "type": "Element",
        },
    )
    linie: Optional[Linie] = field(
        default=None,
        metadata={
            "name": "Linie",
            "type": "Element",
        },
    )
    fahrgastrelevant: Optional[FahrgastrelevantValue] = field(
        default=None,
        metadata={
            "type": "Element",
        },
    )
    lfd_nr_routenvariante: Optional[int] = field(
        default=None,
        metadata={
            "name": "LfdNrRoutenvariante",
            "type": "Element",
        },
    )
    startzeit: Optional[int] = field(
        default=None,
        metadata={
            "name": "Startzeit",
            "type": "Element",
        },
    )
    fahrtart: Optional[str] = field(
        default=None,
        metadata={
            "name": "Fahrtart",
            "type": "Element",
        },
    )
    fahrzeitprofil: Optional[Fahrzeitprofil] = field(
        default=None,
        metadata={
            "name": "Fahrzeitprofil",
            "type": "Element",
        },
    )
    fahrzeugtyp: Optional[str] = field(
        default=None,
        metadata={
            "name": "Fahrzeugtyp",
            "type": "Element",
        },
    )
    fremdunternehmer: Optional[int] = field(
        default=None,
        metadata={
            "name": "Fremdunternehmer",
            "type": "Element",
        },
    )
    auftraggeber: List[int] = field(
        default_factory=list,
        metadata={
            "name": "Auftraggeber",
            "type": "Element",
            "max_occurs": 2,
        },
    )
    veroeffentlicht: List[VeroeffentlichtValue] = field(
        default_factory=list,
        metadata={
            "type": "Element",
            "max_occurs": 2,
        },
    )
    lfd_nr: Optional[int] = field(
        default=None,
        metadata={
            "name": "LfdNr",
            "type": "Element",
        },
    )
    fahrt_id: Optional[int] = field(
        default=None,
        metadata={
            "name": "FahrtID",
            "type": "Element",
        },
    )


@dataclass
class LinienDaten:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    linie: Optional[Linie] = field(
        default=None,
        metadata={
            "name": "Linie",
            "type": "Element",
            "required": True,
        },
    )


@dataclass
class FahrtDaten:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    fahrt: List[Fahrt] = field(
        default_factory=list,
        metadata={
            "name": "Fahrt",
            "type": "Element",
        },
    )


@dataclass
class Fahrtreihenfolge:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    fahrt: List[Fahrt] = field(
        default_factory=list,
        metadata={
            "name": "Fahrt",
            "type": "Element",
            "min_occurs": 1,
        },
    )


@dataclass
class Umlaufteilgruppe:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    lfd_nr: Optional[int] = field(
        default=None,
        metadata={
            "name": "LfdNr",
            "type": "Element",
            "required": True,
        },
    )
    wagenfolgenummer: Optional[int] = field(
        default=None,
        metadata={
            "name": "Wagenfolgenummer",
            "type": "Element",
            "required": True,
        },
    )
    linie: Optional[Linie] = field(
        default=None,
        metadata={
            "name": "Linie",
            "type": "Element",
            "required": True,
        },
    )
    beginn: Optional[int] = field(
        default=None,
        metadata={
            "name": "Beginn",
            "type": "Element",
        },
    )
    ende: Optional[int] = field(
        default=None,
        metadata={
            "name": "Ende",
            "type": "Element",
        },
    )
    fahrzeugtyp: List[str] = field(
        default_factory=list,
        metadata={
            "name": "Fahrzeugtyp",
            "type": "Element",
            "max_occurs": 2,
        },
    )
    fahrtreihenfolge: Optional[Fahrtreihenfolge] = field(
        default=None,
        metadata={
            "name": "Fahrtreihenfolge",
            "type": "Element",
        },
    )


@dataclass
class Umlaufteilgruppen:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    umlaufteilgruppe: List[Umlaufteilgruppe] = field(
        default_factory=list,
        metadata={
            "name": "Umlaufteilgruppe",
            "type": "Element",
            "min_occurs": 1,
        },
    )


@dataclass
class Umlauf:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    lfd_nr: Optional[int] = field(
        default=None,
        metadata={
            "name": "LfdNr",
            "type": "Element",
            "required": True,
        },
    )
    umlauf_id: Optional[int] = field(
        default=None,
        metadata={
            "name": "UmlaufID",
            "type": "Element",
            "required": True,
        },
    )
    umlaufbezeichnung: Optional[str] = field(
        default=None,
        metadata={
            "name": "Umlaufbezeichnung",
            "type": "Element",
            "required": True,
        },
    )
    kalenderdatum: Optional[str] = field(
        default=None,
        metadata={
            "name": "Kalenderdatum",
            "type": "Element",
            "required": True,
        },
    )
    umlaufteilgruppen: Optional[Umlaufteilgruppen] = field(
        default=None,
        metadata={
            "name": "Umlaufteilgruppen",
            "type": "Element",
            "required": True,
        },
    )
    gueltigkeiten: Optional[Gueltigkeiten] = field(
        default=None,
        metadata={
            "name": "Gueltigkeiten",
            "type": "Element",
            "required": True,
        },
    )


@dataclass
class Umlaeufe:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    umlauf: List[Umlauf] = field(
        default_factory=list,
        metadata={
            "name": "Umlauf",
            "type": "Element",
            "min_occurs": 1,
        },
    )


@dataclass
class Fahrzeugumlauf:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    lfd_nr: Optional[int] = field(
        default=None,
        metadata={
            "name": "LfdNr",
            "type": "Element",
            "required": True,
        },
    )
    betriebshof: Optional[int] = field(
        default=None,
        metadata={
            "name": "Betriebshof",
            "type": "Element",
            "required": True,
        },
    )
    fahrzeugtyp: Optional[str] = field(
        default=None,
        metadata={
            "name": "Fahrzeugtyp",
            "type": "Element",
            "required": True,
        },
    )
    umlaeufe: Optional[Umlaeufe] = field(
        default=None,
        metadata={
            "name": "Umlaeufe",
            "type": "Element",
            "required": True,
        },
    )


@dataclass
class FahrzeugumlaufDaten:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    fahrzeugumlauf: List[Fahrzeugumlauf] = field(
        default_factory=list,
        metadata={
            "name": "Fahrzeugumlauf",
            "type": "Element",
        },
    )


@dataclass
class Linienfahrplan:
    class Meta:
        namespace = "http://www.ivu.de/mb/intf/passengercount/remote/model/"

    generierung: Optional[Generierung] = field(
        default=None,
        metadata={
            "name": "Generierung",
            "type": "Element",
            "required": True,
        },
    )
    streckennetz_daten: Optional[StreckennetzDaten] = field(
        default=None,
        metadata={
            "name": "StreckennetzDaten",
            "type": "Element",
        },
    )
    linien_daten: Optional[LinienDaten] = field(
        default=None,
        metadata={
            "name": "LinienDaten",
            "type": "Element",
        },
    )
    fahrt_daten: Optional[FahrtDaten] = field(
        default=None,
        metadata={
            "name": "FahrtDaten",
            "type": "Element",
        },
    )
    fahrzeugumlauf_daten: Optional[FahrzeugumlaufDaten] = field(
        default=None,
        metadata={
            "name": "FahrzeugumlaufDaten",
            "type": "Element",
        },
    )
