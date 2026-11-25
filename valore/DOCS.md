# Valore IP07/X6

## Konfigurace

- **E-mail a heslo účtu Tuya:** použij účet, na kterém je spárovaný Valore.
  Přihlašovací údaje ulož v konfiguraci aplikace v Home Assistantu.
- **Kód země:** pro český účet zadej `420`.
- **ID zvonku:** vyplň, pokud účet obsahuje více zařízení. Jinak aplikace
  vybere jediné zařízení s lokálním klíčem automaticky.
- **Oznámení na iPhonu:** do `notify_service` vlož přesný název akce
  `notify.mobile_app_…` z nabídky **Nástroje pro vývojáře → Akce**.
  I bez vyplnění pole aplikace předává stisk jako událost `valore_ring`.

## Ověření

Ukonči případný server Valore na Macu a v Home Assistantu spusť aplikaci.
Zapni **Zobrazit v postranním panelu**. Po fyzickém zazvonění otevři
**Live video**. Pro mluvení otevři Home Assistant přes HTTPS, povol
mikrofon a podrž **Podržet a mluvit**.

Nejprve v panelu spusť **Poslat zkušební oznámení**. Potom fyzicky
zazvoň a ověř oznámení na telefonu. Běžné čtení událostí Tuya probíhá
po 20 sekundách. Oznámení vyvolá stisk tlačítka. Zpracování obrazu
a zvuku běží v Home Assistant OS a připojení zvonku využívá cloud Tuya.

Živý obraz a mluvení jsou dostupné v panelu Valore. Tato verze poskytuje
událost pro automatizace Home Assistantu.
