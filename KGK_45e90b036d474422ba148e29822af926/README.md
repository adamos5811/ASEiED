# Analiza danych w Spark/Scala

## Uruchomienie projektu

Do uruchomienia programu wymagany jest zainstalowany Maven.

Otwieramy projekt w Netbeansie i klikamy Build lub prawym klawiszem na nazwę projektu a następnie Test.

Wyniki zapisywane są do plików w folderze "src/main/resources".

Do wygenerowania wykresu dodatkowo potrzebny jest Python.

Gdy pliki wynikowe są już w folderze, uruchamiamy znajdujący się także w nim skrypt "wykres.py".

Wykres generowany jest do pliku "wykres.pdf".

## Działenie programu

Program wczytuje dane z udostępnionego nam pliku JSON. Niestety scala nie jest w stanie sparsować tak sformatowanego JSONa w prosty sposób. Potrzebuje pliku, w którym jsonowe obiekty znajdują się w pojedynczych liniach. Taki plik jest tworzony na początku ("dane2.json").

Następnie wczytujemy dane i wykonujemy operacje zawarte w treści zadania:
  - policzenie osób pracujących w danym mieście,
  - policzenie osób pracujących w danej firmie,
  - policzenie osób o danym imieniu.
  
 Wyniki powyższych działań zapisane są do odpowiednich plików tekstowych w folderze "resources".
 
 Przygotowywany jest także plik potrzebny do przygotowania wykresu występowania określonych imion w danej lokalizacji (wynik.txt), w którym jest lista zawierająca [imię, miasto, liczbę osób o tym imieniu w tym mieście].
 
 Na podstawie tego pliku dostępny skrypt Pythonowy tworzy wykres przedstawający te dane.
 
 
