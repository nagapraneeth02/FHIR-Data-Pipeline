import pandas as pd
import os
import json
import re
from datetime import datetime, date
from fhir.resources.patient import Patient
from fhir.resources.encounter import Encounter, EncounterDiagnosis
from fhir.resources.procedure import Procedure
from fhir.resources.claim import Claim
from fhir.resources.reference import Reference
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.coding import Coding
from fhir.resources.codeablereference import CodeableReference
from fhir.resources.period import Period
from fhir.resources.money import Money

def clean_id(raw_id):
    return re.sub(r'[^A-Za-z0-9\-.]', '-', str(raw_id))

def csv_to_fhir(input_path, output_path):
    df = pd.read_csv(input_path, encoding='latin1', low_memory=False)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    fhir_resources = []

    for _, row in df.iterrows():
        # Patient
        patient = Patient(
            id=str(row['MEMBER_KEY']),
            identifier=[{
                "system": "http://example.com/member_id",
                "value": str(row['MEMBER_ID'])
            }],
            gender=row['MEM_GENDER'].lower() if pd.notna(row['MEM_GENDER']) else None
        )
        if pd.notna(row['MEM_DOB']):
            patient.birthDate = datetime.strptime(row['MEM_DOB'], '%m/%d/%Y').date().isoformat()
        if pd.notna(row['MEM_ZIP']):
            patient.address = [{"postalCode": str(row['MEM_ZIP'])}]
        fhir_resources.append(patient.dict())

        # Encounter
        encounter_id = clean_id(f"enc-{row['CLAIM_ID']}")
        encounter = Encounter(
            id=encounter_id,
            status="finished",
            subject=Reference(reference=f"Patient/{row['MEMBER_KEY']}")
        )

        if pd.notna(row.get('Inpatient/ outpatient')):
            encounter.type = [CodeableConcept(
                coding=[Coding(
                    system="http://terminology.hl7.org/CodeSystem/v3-ActCode",
                    code="IMP" if row['Inpatient/ outpatient'] == "Inpatient" else "AMB"
                )]
            )]

        if pd.notna(row['AdmitDate']) and pd.notna(row['DischargeDate']):
            encounter.actualPeriod = Period(
                start=datetime.strptime(row['AdmitDate'], '%m/%d/%Y').isoformat(),
                end=datetime.strptime(row['DischargeDate'], '%m/%d/%Y').isoformat()
            )

        if pd.notna(row.get('DRG_CODE')):
            diagnosis = EncounterDiagnosis(
                condition=[CodeableReference(reference=Reference(reference=f"Condition/cond-{row['CLAIM_ID']}-primary"))],
                use=[CodeableConcept(
                    coding=[Coding(
                        system="http://hl7.org/fhir/diagnosis-role",
                        code="billing"
                    )]
                )]
            )
            encounter.diagnosis = [diagnosis]

        fhir_resources.append(encounter.dict())

        # Procedure
        for i in range(1, 5):
            proc_code = row.get(f'proc{i}_code')
            proc_desc = row.get(f'proc{i}_desc')
            if pd.notna(proc_code):
                procedure = Procedure(
                    id=clean_id(f"proc-{row['CLAIM_ID']}-{i}"),
                    status="completed",
                    subject=Reference(reference=f"Patient/{row['MEMBER_KEY']}"),
                    encounter=Reference(reference=f"Encounter/{encounter_id}"),
                    code=CodeableConcept(
                        coding=[Coding(
                            system="http://www.ama-assn.org/go/cpt",
                            code=str(proc_code),
                            display=proc_desc if pd.notna(proc_desc) else None
                        )]
                    )
                )
                fhir_resources.append(procedure.dict())

        # Claim
        claim = Claim(
            id=str(row['CLAIM_ID']),
            status="active",
            use="claim",
            created=str(date.today()),
            patient=Reference(reference=f"Patient/{row['MEMBER_KEY']}"),
            encounter=[Reference(reference=f"Encounter/{encounter_id}")],
            type=CodeableConcept(
                coding=[Coding(
                    system="http://terminology.hl7.org/CodeSystem/claim-type",
                    code="institutional"
                )]
            )
        )
        if pd.notna(row['AMT_BILLED']):
            claim.total = Money(value=float(row['AMT_BILLED']), currency="USD")
        if pd.notna(row['PayerType']):
            claim.insurer = {"display": row['PayerType']}
        fhir_resources.append(claim.dict())

    with open(output_path, 'w') as f:
        json.dump(fhir_resources, f, indent=2, default=str)
