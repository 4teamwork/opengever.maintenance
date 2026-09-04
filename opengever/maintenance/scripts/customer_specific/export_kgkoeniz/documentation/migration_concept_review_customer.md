# Review Migrationskonzept: Abweichungen Konzept vs. Umsetzung

Quelle Original: `2.2.2 Konzept Datenlieferung OneGov GEVER-v2-20260512_101839.docx`
Umgesetzte Spezifikation: `migration_concept.md`

Dieses Dokument stellt die Punkte gegenüber, in denen die Umsetzung bewusst vom ursprünglichen Konzept abweicht, gruppiert nach Art der Abweichung, mit kurzer Begründung. Massgeblich für die technische Umsetzung ist `migration_concept.md`; das vorliegende Dokument dient der Nachvollziehbarkeit der Änderungen gegenüber der ursprünglichen Vorgabe.

---

## 1. Modellierung von 1:n-Beziehungen

**Original:** Die Zuordnung von "vielen" Datensätzen (n) zu einem übergeordneten Datensatz (1) wird uneinheitlich gehandhabt. Meistens werden die n-Datensätze als Pipe-getrennte Liste (Label und UID) auf der Zeile des übergeordneten (1er-)Datensatzes geführt, z.B. listet ein Dossier seine Schlagwörter, verwandten Dossiers, Subdossiers, Teamräume, Beteiligungen und Kommentare jeweils als Multi-Value-Spaltenpaar auf sich selbst. An anderen Stellen ist es genau umgekehrt: eine Aufgabe trägt ihr übergeordnetes Dossier als eigenes Feld auf der Aufgaben-Zeile.

**Umsetzung:** Durchgängig normalisiert: Der referenzierende Datensatz auf der "n"-Seite trägt die UID des übergeordneten ("1er"-)Datensatzes, nicht umgekehrt. Beispiel: `documents.csv` enthält pro Dokument die UID seines Dossiers (`Übergeordnetes Dossier - UID`), nicht `dossiers.csv` eine Liste aller Dokument-UIDs. Gleiches gilt für die Ordnungssystem-Hierarchie, Dossier-Hierarchie, Aufgaben-Hierarchie sowie Kommentare und Beteiligungen (siehe Abschnitt 2).

**Begründung:** Ein Datensatz kennt zu jedem Zeitpunkt nur genau einen Elternteil, aber ein Elternteil potenziell beliebig viele Kinder. Die Referenz auf der Kind-Seite zu führen ist die eindeutige, redundanzfreie Darstellung dieser Beziehung und entspricht der Standard-Modellierung 1:n-Beziehungen in relationalen Daten. Zusätzlich ermöglicht dieses Muster eine automatisierte Prüfung: jede referenzierte UID wird beim Export gegen die tatsächlich exportierten Datensätze der Zieltabelle abgeglichen, was mit einer Liste auf der 1er-Seite nicht in gleicher Konsistenz möglich wäre.

Echte Mehrfachbeziehungen (m:n), bei denen ein Datensatz tatsächlich zu mehreren Elternteilen gehören kann (z.B. Dossier↔Schlagwort, Aufgabe↔Informierte Beteiligte, Sitzung↔Sitzungsmitglied), werden weiterhin als Pipe-getrennte Multi-Value-Spalte geführt – das entspricht dem Original und ist hier weiterhin sinnvoll, da eine eigene Verknüpfungstabelle für diese Fälle keinen Mehrwert bietet. Eine Ausnahme bilden die Beteiligungen (`participations.csv`): Da hier zusätzlich eine Rolle pro Verknüpfung (Dossier–Person) erfasst wird, ist eine echte Verknüpfungstabelle mit einer Zeile pro Beteiligung die korrekte Modellierung.

---

## 2. Aufgelöste doppelte Modellierung bei Kommentaren und Beteiligungen

**Original:** Bei Kommentaren und Beteiligungen wird dieselbe Beziehung zum Dossier doppelt geführt: Das Dossier listet seine Kommentare bzw. Beteiligungen als Multi-Value-Spaltenpaar auf sich selbst, **und** der Kommentar- bzw. Beteiligungs-Datensatz trägt zusätzlich unabhängig eine eigene `Dossier - UID`-Spalte zurück zum Dossier.

**Umsetzung:** Die Beziehung wird nur einmal geführt – als Referenz auf der Kind-Seite (`comments.csv` bzw. `participations.csv` referenzieren ihr Dossier), analog zu Abschnitt 1.

**Begründung:** Zwei unabhängige Spalten für dieselbe Beziehung können bei der Datenaufbereitung auseinanderlaufen (z.B. wenn ein Kommentar in der Liste des Dossiers fehlt, aber seine eigene Dossier-UID gesetzt ist, oder umgekehrt). Eine einzige, eindeutige Quelle pro Beziehung schliesst diese Fehlerquelle aus.

---

## 3. Neu ergänzte Tabellen

**Ordnungssystem (`repository.csv`):** Im Original existiert keine eigene Exporttabelle für das Ordnungssystem – Dossiers referenzieren lediglich eine `Ordnungsposition UID`, ohne dass die Ordnungspositionen selbst (Titel, Hierarchie etc.) exportiert würden. Diese Tabelle wurde ergänzt, da die Ordnungsstruktur inhaltlich zur Ordnungsposition gehört und ohne sie nicht rekonstruierbar ist.

**Sitzungsmitglieder (`members.csv`):** Im Original werden Vorsitz und Teilnehmende einer Sitzung nur als Klartext-Namen geführt, ohne eigene, referenzierbare Datensätze. Diese Tabelle wurde ergänzt, damit Vorsitz und Teilnehmende wie alle anderen Beziehungen über eine stabile ID referenziert werden können, statt über einen Namen, der z.B. bei Schreibvarianten oder Namensänderungen nicht eindeutig ist.

---

## 4. Entfernte Duplizierung von Referenzfeldern (Label + UID)

**Original:** Jedes referenzierende Feld (`Assoc`) wird als Paar geliefert – eine Spalte mit dem lesbaren Label (z.B. `Federführend`, `Schlagwörter`, `Beteiligungen`) und eine separate Spalte mit der zugehörigen UID (`Federführend - Benutzer UID`, `Schlagwörter UID`, `Beteiligungen - UID`).

**Umsetzung:** Es wird nur die UID-Spalte geliefert. Das Label ist über die referenzierte Zieltabelle nachschlagbar (z.B. der Benutzername über `users.csv`, das Schlagwort über `keywords.csv`).

**Begründung:** Die Duplizierung von Label und UID im selben Export ist eine Redundanz, die bei jeder Änderung des Quelldatensatzes (z.B. Umbenennung eines Schlagworts oder Benutzers) potenziell zu widersprüchlichen Ständen zwischen den beiden Spalten führen kann. Da die referenzierten Stammdaten ohnehin vollständig exportiert werden, lässt sich das Label jederzeit eindeutig über die UID nachschlagen – die Information geht nicht verloren, sondern liegt an genau einer Stelle.

---

## 5. Ergänzte fehlende Referenz bei Dokumenten

**Original:** Die Dokumente-Tabelle enthält keine Referenz auf das übergeordnete Dossier. Die Zuordnung eines Dokuments zu seinem Dossier ist im Original nicht abgebildet.

**Umsetzung:** `documents.csv` enthält eine Referenz auf genau einen übergeordneten Container – `Übergeordnetes Dossier - UID`, `Übergeordnete Aufgabe - UID` oder `Übergeordneter Antrag - UID`, je nachdem wo das Dokument tatsächlich abgelegt ist.

**Begründung:** Ohne diese Referenz liesse sich aus dem Export nicht rekonstruieren, zu welchem Dossier, welcher Aufgabe oder welchem Antrag ein Dokument gehört – eine für die Migration unverzichtbare Information.

---

## 6. Bereinigte bzw. nicht übernommene Felder

Mehrere Felder waren im Original mit „-" (Lieferung/Feldtyp noch nicht definiert) markiert oder betreffen Konzepte, die in CMI kein Äquivalent haben. Diese wurden entweder aufgelöst oder bewusst nicht übernommen:

| Feld (Original) | Umgang in der Umsetzung | Begründung |
|---|---|---|
| Klassifikation, Datenschutz, Öffentlichkeitsstatus, Aufbewahrungsdauer, Kommentar zur Aufbewahrungsdauer, Archivwürdigkeit, Kommentar zur Archivwürdigkeit, Archivische Schutzfrist, Titel der Ordnungsposition (französisch/englisch) (bei Dossier mit „-" markiert) | Nicht übernommen | Diese Attribute gehören inhaltlich zur Ordnungsposition, sind aber für die Migration nicht relevant und werden bewusst nicht exportiert. |
| Verknüpfte Teamräume (Titel + UID) | Nicht übernommen | Teamräume/Workspaces sind kein Bestandteil dieses Exports; es existiert kein entsprechendes CMI-Zielkonzept im Rahmen dieser Datenlieferung. |
| Ablage-Präfix, Behältnis-Art, Anzahl Behältnisse, Behältnis Standort, Externe Referenz, Dossiertyp | Nicht übernommen (im Original grösstenteils undefiniert oder ohne Beispielwert) | Physische Ablageattribute ohne digitales Äquivalent bzw. im Original nicht spezifiziert. |
| Dateigrösse, Version, Genehmigung, Eingereicht bei, Antrag/Eingereichter Antrag/Sitzung als eigene Felder (bei Dokumente, alle im Original mit „-" markiert) | Nicht übernommen; Zuordnung zu Antrag/Aufgabe erfolgt stattdessen über die neu ergänzte Parent-Referenz (Abschnitt 5) | Im Original nicht spezifiziert; die relevante Information (Zugehörigkeit zu Antrag/Aufgabe) wird bereits durch die neue Parent-Referenz abgedeckt. |
| Status (Dossier, Aufgabe – im Original teils mit „-" markiert) | Als übersetzter Klartextwert geliefert (z.B. "In Bearbeitung") | Wert war im Original nicht spezifiziert, wird aber für die Migration benötigt und ist eindeutig aus dem Workflow-Status ableitbar. |

---

## 7. Formatentscheide

- **Dateiformat:** Nur CSV statt der im Original offengelassenen Wahl zwischen CSV und Excel. Ein einziges, maschinell eindeutig verarbeitbares Format reduziert Interpretationsspielraum und vereinfacht die automatisierte Validierung.
- **Trennzeichen:** `;` als Spaltentrennzeichen (im Original nicht spezifiziert).
- **Blob-Verzeichnis für Dokumente:** Benannt nach der Dokument-**UID** statt der Dokument-**ID** wie im Original vorgeschlagen. Die UID ist ein stabiler, garantiert eindeutiger Identifikator, während die ID-Eindeutigkeit im Original lediglich angenommen wurde.
- **Selbstdokumentierende Spaltenköpfe:** Referenzierende Spalten tragen den Namen der Zieldatei im Spaltenkopf (z.B. `Übergeordnetes Dossier - UID -- dossiers`). Diese Konvention war im Original nicht vorgesehen, macht die CSV-Dateien aber ohne Zusatzdokumentation eigenständig verständlich.

---

## 8. Offener Punkt: Gremien (Committees)

Im Original ist für Gremien nur ein leerer Platzhalter-Abschnitt ohne Tabelle vorhanden – die Struktur war zum Zeitpunkt der Konzepterstellung noch nicht ausgearbeitet. In der Umsetzung existiert ebenfalls kein eigener Gremien-Export: Das Gremium einer Sitzung wird in `meetings.csv` weiterhin als reiner Klartextwert (`Gremium`) geführt, nicht als Referenz auf einen eigenständigen Datensatz.

Dies ist keine Änderung gegenüber dem Original, sondern eine unverändert offene Lücke aus dem ursprünglichen Konzept. Sie wird hier bewusst aufgeführt, falls für die Migration eine strukturierte Gremien-Zuordnung (z.B. für spätere Referenzierbarkeit) benötigt wird – in diesem Fall müsste analog zu Abschnitt 3 ein eigener Export ergänzt werden.

---

Massgeblich für die Umsetzung bleibt `migration_concept.md` sowie der Code unter `src/opengever.maintenance/opengever/maintenance/scripts/export_gever_koeniz`.
