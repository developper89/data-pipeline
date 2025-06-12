# Analyse du Format Wire Protobuf : Extraction d'ID d'Appareil

Ce document explique comment nous analysons les charges utiles binaires protobuf pour extraire les ID d'appareils des requêtes CoAP, en se concentrant sur le protocole de format wire et la logique d'analyse exacte.

## Table des Matières

- [Aperçu](#aperçu)
- [Bases du Format Wire Protobuf](#bases-du-format-wire-protobuf)
- [Types Wire Expliqués](#types-wire-expliqués)
- [Exemple Réel : Extraction d'ID d'Appareil](#exemple-réel--extraction-did-dappareil)
- [Processus d'Analyse Étape par Étape](#processus-danalyse-étape-par-étape)
- [Implémentation du Code](#implémentation-du-code)
- [Pourquoi les Champs 1 et 16 ?](#pourquoi-les-champs-1-et-16-)
- [Dépannage](#dépannage)

## Aperçu

Lorsque les appareils IoT envoient des données via CoAP, ils utilisent souvent Protocol Buffers (protobuf) pour encoder leurs messages dans un format binaire compact. Pour extraire les ID d'appareils de ces messages, nous devons comprendre le **format wire protobuf** - l'encodage de bas niveau qui définit comment les données sont stockées en binaire.

Notre système extrait avec succès des ID d'appareils comme `282c02424eed` à partir de charges utiles binaires de 58 octets envoyées par des appareils IoT externes.

## Bases du Format Wire Protobuf

### Structure de l'Octet Tag

Chaque champ protobuf commence par un **octet tag** qui encode deux informations :

```
Octet Tag (8 bits) : FFFFFTTT
                     │││││└┴┴─── Type Wire (bits 0-2): Comment lire les données
                     └┴┴┴┴────── Numéro de Champ (bits 3-7): Quel champ c'est
```

### Extraction d'Informations de l'Octet Tag

```python
tag = payload[offset]                # Lire l'octet tag
field_number = tag >> 3             # Décaler à droite de 3 bits pour obtenir le numéro de champ
wire_type = tag & 0x07              # Masquer les 3 bits inférieurs pour obtenir le type wire
```

**Exemple** : Octet tag `0x0a` (décimal 10, binaire `00001010`)

- Numéro de champ : `00001` = 1
- Type wire : `010` = 2

## Types Wire Expliqués

Protobuf définit 5 types wire qui indiquent à l'analyseur comment lire les données suivantes :

| Type Wire | Nom                       | Description                        | Format de Données                    |
| --------- | ------------------------- | ---------------------------------- | ------------------------------------ |
| **0**     | Varint                    | Entier de longueur variable        | 1-10 octets, bit de continuation MSB |
| **1**     | 64-bit                    | Nombre 64-bit fixe                 | Toujours exactement 8 octets         |
| **2**     | **Délimité par longueur** | **Chaîne/octets/message imbriqué** | **Octet de longueur + données**      |
| **3**     | Début de groupe           | Obsolète                           | (Non utilisé)                        |
| **4**     | Fin de groupe             | Obsolète                           | (Non utilisé)                        |
| **5**     | 32-bit                    | Nombre 32-bit fixe                 | Toujours exactement 4 octets         |

### Type Wire 2 : Délimité par Longueur (Le Plus Important)

Le **type wire 2** est crucial pour l'extraction d'ID d'appareil car les chaînes et tableaux d'octets utilisent ce format :

```
Structure : [Tag][Longueur][Données...]
Exemple :    0a     06      282c02424eed
            ↑      ↑       ↑
          Champ1  6octets  ID d'appareil réel
          Type2
```

**Protocole** :

1. Lire l'octet tag pour identifier le champ et le type wire
2. Lire l'octet suivant comme **longueur** (nombre d'octets de données qui suivent)
3. Lire **exactement ce nombre d'octets** comme données du champ

## Exemple Réel : Extraction d'ID d'Appareil

### Données Réelles de Charge Utile CoAP

D'un vrai appareil IoT (`80.187.67.10`) :

```
Hex Brut : 0a06282c02424eed1001183c220f080110b8d0a7c20620d2032a020000220e080210b8d0a7c20620722a0200002896d5a7c20640014809820100
Longueur : 58 octets
```

### Analyse du Champ 1 (ID d'Appareil)

Traçons le premier champ étape par étape :

```
Position : [0] [1] [2] [3] [4] [5] [6] [7] [8] [9] [10] ...
Hex :      0a  06  28  2c  02  42  4e  ed  10  01  18  ...
```

## Processus d'Analyse Étape par Étape

### Étape 1 : Analyser l'Octet Tag (Position 0)

```python
tag = 0x0a  # = 10 en décimal = 00001010 en binaire

# Extraire le numéro de champ et le type wire en utilisant des opérations de bits
field_number = 0x0a >> 3  # Décaler à droite de 3 bits : 00001010 → 00001 = 1
wire_type = 0x0a & 0x07   # Masquer les 3 bits inférieurs : 00001010 & 00000111 = 010 = 2
```

**Résultat** : Champ 1, Type wire 2 (délimité par longueur)

### Étape 2 : Lire l'Octet de Longueur (Position 1)

Puisque type wire = 2, le protocole spécifie :

- Octet suivant = longueur des données qui suivent
- Octets suivants = données réelles du champ

```python
length = 0x06  # Position 1 = 6 octets de données suivront
```

### Étape 3 : Extraire les Données d'ID d'Appareil (Positions 2-7)

```python
# Lire exactement 6 octets comme spécifié par l'octet de longueur
device_id_bytes = [0x28, 0x2c, 0x02, 0x42, 0x4e, 0xed]

# Convertir chaque octet en représentation hex de 2 caractères
hex_chars = []
for byte in device_id_bytes:
    hex_chars.append(f"{byte:02x}")

device_id = "".join(hex_chars)  # Résultat : "282c02424eed"
```

### Étape 4 : Vérification

**Conversion Binaire vers Hex** :

- `0x28` → `"28"`
- `0x2c` → `"2c"`
- `0x02` → `"02"`
- `0x42` → `"42"`
- `0x4e` → `"4e"`
- `0xed` → `"ed"`

**ID d'Appareil Final** : `"282c02424eed"` (12 caractères représentant 6 octets)

## Implémentation du Code

### Méthode d'Extraction Primaire

```python
def _parse_protobuf_field(self, payload: bytes, field_number: int) -> str:
    """Analyser un champ protobuf spécifique et retourner sa valeur chaîne."""
    try:
        offset = 0
        while offset < len(payload):
            if offset >= len(payload):
                break

            # Lire le tag (numéro de champ + type wire)
            tag = payload[offset]
            found_field_number = tag >> 3
            wire_type = tag & 0x07
            offset += 1

            # Vérifier si c'est le champ que nous cherchons
            if found_field_number == field_number and wire_type == 2:  # Délimité par longueur
                if offset >= len(payload):
                    break

                # Lire l'octet de longueur
                length = payload[offset]
                offset += 1

                # Lire les données réelles
                if offset + length <= len(payload):
                    field_data = payload[offset:offset + length]

                    # Essayer de décoder comme chaîne UTF-8 d'abord
                    try:
                        device_id = field_data.decode('utf-8')
                        if device_id and device_id.isprintable():
                            return device_id
                    except UnicodeDecodeError:
                        # Si pas UTF-8, retourner comme chaîne hex
                        return field_data.hex()

                break

            # Ignorer la valeur de ce champ basée sur le type wire
            elif wire_type == 0:  # Varint
                while offset < len(payload) and payload[offset] & 0x80:
                    offset += 1
                offset += 1
            elif wire_type == 1:  # 64-bit
                offset += 8
            elif wire_type == 2:  # Délimité par longueur (pas notre champ cible)
                if offset >= len(payload):
                    break
                length = payload[offset]
                offset += 1 + length
            elif wire_type == 5:  # 32-bit
                offset += 4
            else:
                break  # Type wire inconnu

    except Exception as e:
        logger.debug(f"L'analyse du champ protobuf {field_number} a échoué : {e}")

    return None
```

### Logique Complète d'Extraction d'ID d'Appareil

```python
def _extract_device_id_from_payload(self, payload: bytes, request_id: str) -> str:
    """Extraire l'ID d'appareil de la charge utile protobuf en utilisant plusieurs stratégies."""
    try:
        # Stratégie 1 : Essayer le champ protobuf 1 (emplacement le plus courant)
        device_id = self._parse_protobuf_field(payload, field_number=1)
        if device_id:
            logger.info(f"🔍 ID d'appareil trouvé dans le champ protobuf 1 : '{device_id}'")
            return device_id

        # Stratégie 2 : Essayer le champ 16 comme solution de secours (champ de métadonnées)
        device_id = self._parse_protobuf_field(payload, field_number=16)
        if device_id:
            logger.info(f"🔍 ID d'appareil trouvé dans le champ protobuf 16 : '{device_id}'")
            return device_id

        # Stratégie 3 : Correspondance de motifs d'octets bruts en dernier recours
        device_id = self._extract_device_id_from_raw_bytes(payload)
        if device_id:
            logger.info(f"🔍 ID d'appareil trouvé dans les octets bruts : '{device_id}'")
            return device_id

    except Exception as e:
        logger.info(f"❌ L'extraction de l'ID d'appareil a échoué : {e}")

    return None
```

## Pourquoi les Champs 1 et 16 ?

### Champ 1 : Cible Primaire

- **Convention** : Le champ 1 contient généralement l'identifiant primaire dans les schémas protobuf
- **Preuve** : Nos données CoAP réelles montrent l'ID d'appareil `282c02424eed` dans le champ 1
- **Efficacité** : Analyse rapide car c'est généralement le premier champ

### Champ 16 : Cible de Secours

- **Métadonnées** : Les numéros de champs plus élevés contiennent souvent des métadonnées d'appareil
- **Flexibilité** : Certains fabricants IoT pourraient utiliser des dispositions de champs différentes
- **Robustesse** : Fournit une méthode d'extraction de sauvegarde

### La Stratégie à Trois Couches

```python
# Couche 1 : Champ protobuf 1 (primaire)
device_id = parse_protobuf_field(payload, field_number=1)

# Couche 2 : Champ protobuf 16 (secours métadonnées)
if not device_id:
    device_id = parse_protobuf_field(payload, field_number=16)

# Couche 3 : Correspondance de motifs d'octets bruts (dernier recours)
if not device_id:
    device_id = extract_from_raw_bytes(payload)
```

## Dépannage

### Problèmes Courants et Solutions

1. **"L'analyse du type wire a échoué"**

   - **Cause** : Charge utile corrompue ou non-protobuf
   - **Solution** : La correspondance de motifs d'octets bruts se déclenche automatiquement

2. **"Champ non trouvé"**

   - **Cause** : L'appareil utilise un schéma protobuf différent
   - **Solution** : Le système essaie le champ 16, puis les octets bruts

3. **"Conversion hex invalide"**

   - **Cause** : Données binaires non-UTF8 dans le champ ID d'appareil
   - **Solution** : Le code retourne la représentation hex à la place

4. **"L'extraction de l'ID d'appareil a échoué"**
   - **Cause** : La charge utile ne contient pas d'identifiant d'appareil reconnaissable
   - **Solution** : Enregistrer les détails pour analyse manuelle

### Techniques de Débogage

```python
# Activer la journalisation détaillée pour voir les étapes d'analyse
logger.info(f"Octet tag : 0x{tag:02x} → champ={field_number}, type_wire={wire_type}")
logger.info(f"Octet longueur : {length} → lecture de {length} octets")
logger.info(f"Données brutes du champ : {field_data.hex()}")
logger.info(f"ID d'appareil décodé : '{device_id}'")
```

### Analyse de Charge Utile

Le système enregistre automatiquement une analyse complète de la charge utile :

```
🔬 ANALYSE DE CHARGE UTILE [REQ-0001]:
  Premiers octets : 0a06282c02424eed (['0xa', '0x6', '0x28', '0x2c', '0x2', '0x42', '0x4e', '0xed'])
  🔍 Possible Protocol Buffers (protobuf) - commence par 0x0a
  📦 Analyse Protobuf :
    Champ 1 : numéro=1, type_wire=2
    Champ 2 : numéro=2, type_wire=0
    Champ 3 : numéro=3, type_wire=0
    9 champs protobuf trouvés
```

## Considérations de Performance

- **Analyse rapide** : Le champ 1 est généralement trouvé immédiatement
- **Efficace en mémoire** : Traite la charge utile séquentiellement sans mise en mémoire tampon
- **Résistant aux erreurs** : Plusieurs stratégies de secours empêchent les échecs
- **Surcharge minimale** : Analyse seulement les champs nécessaires pour l'extraction d'ID d'appareil

## Notes de Sécurité

- **Validation d'entrée** : Toutes les opérations d'octets incluent la vérification des limites
- **Gestion d'erreur** : Les charges utiles mal formées ne feront pas planter le système
- **Journalisation** : Toutes les tentatives d'extraction sont enregistrées pour audit de sécurité
- **Aucune dépendance externe** : Utilise seulement les bibliothèques Python standard pour l'analyse

## Résultats de l'Analyse du Champ 16

Lors de notre analyse de curiosité du champ 16 dans la charge utile réelle, nous avons découvert :

```
🎯 CHAMP 16 TROUVÉ !
   Longueur : 1 octet
   Données brutes : 00
   Octets bruts : [0]
   📝 Représentation hex : '00'
```

**Conclusion** : Le champ 16 contient une seule valeur nulle (`0x00`), probablement un indicateur d'état ou un champ de métadonnées réservé. Cela confirme que le champ 1 est effectivement l'emplacement correct pour l'ID d'appareil dans ce schéma protobuf particulier.
