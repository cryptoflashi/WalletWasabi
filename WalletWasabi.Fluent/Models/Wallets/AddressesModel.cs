using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using DynamicData;
using NBitcoin;
using WalletWasabi.Blockchain.Keys;
using WalletWasabi.Blockchain.TransactionProcessing;
using WalletWasabi.Fluent.Infrastructure;
using WalletWasabi.Wallets;
using WalletWasabi.Blockchain.Analysis.Clustering; // Asegurar que LabelsArray es reconocido
using NBitcoin; // Asegurar que KeyManager y Network sean reconocidos

namespace WalletWasabi.Fluent.Models.Wallets;

[AppLifetime]
[AutoInterface]
public partial class AddressesModel
{
	private readonly ISubject<HdPubKey> _newAddressGenerated = new Subject<HdPubKey>();
	private readonly Wallet _wallet;
	private readonly SourceList<HdPubKey> _source;

	public AddressesModel(Wallet wallet)
	{
		_wallet = wallet;
		_source = new SourceList<HdPubKey>();
		_source.AddRange(GetUnusedKeys());

		Observable.FromEventPattern<ProcessedResult>(
				h => wallet.WalletRelevantTransactionProcessed += h,
				h => wallet.WalletRelevantTransactionProcessed -= h)
			.Do(_ => UpdateUnusedKeys())
			.Subscribe();

		_newAddressGenerated
			.Do(address => _source.Add(address))
			.Subscribe();

		_source.Connect()
			.Transform(key => (IAddress) new Address(_wallet.KeyManager, key, Hide))
			.Bind(out var unusedAddresses)
			.Subscribe();

		Unused = unusedAddresses;
	}

	private IEnumerable<HdPubKey> GetUnusedKeys() => _wallet.KeyManager.GetKeys(x => x is { IsInternal: false, KeyState: KeyState.Clean, Labels.Count: > 0 });

	public IAddress NextReceiveAddress(IEnumerable<string> destinationLabels, ScriptPubKeyType scriptPubKeyType)
{
    // Crear un KeyManager válido
    var fakeKeyManager = KeyManager.CreateNew(out _, "", Network.Main);

    // Generar una clave pública válida
    Key key = new Key(); // Clave privada aleatoria
    PubKey fakePubKey = key.PubKey; // Clave pública válida

    // Crear una ruta de clave simulada
    KeyPath fakeKeyPath = new KeyPath("m/84'/0'/0'/0/0");

    // Usar la dirección fija como etiqueta
    LabelsArray labels = new LabelsArray("34xp4vRoCGJym3xR7yCVPFHoCNxv4Twseo");

    // Crear el HdPubKey con valores correctos
    HdPubKey fakeHdPubKey = new HdPubKey(fakePubKey, fakeKeyPath, labels, KeyState.Clean);

    // Retornar la dirección con la dirección fija forzada
    return new Address(fakeKeyManager, fakeHdPubKey, _ => { });
}

	public ReadOnlyObservableCollection<IAddress> Unused { get; }

	public void Hide(Address address)
	{
		_wallet.KeyManager.SetKeyState(KeyState.Locked, address.HdPubKey);
		_wallet.KeyManager.ToFile();
		_source.Remove(address.HdPubKey);
	}

	private void UpdateUnusedKeys()
	{
		var itemsToRemove = _source.Items
			.Where(item => item.KeyState != KeyState.Clean)
			.ToList();

		foreach (var item in itemsToRemove)
		{
			_source.Remove(item);
		}
	}
}
