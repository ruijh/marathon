package mesosphere.marathon.core.volume

import com.wix.accord._
import mesosphere.marathon.core.volume.providers._
import mesosphere.marathon.state._
import org.apache.mesos.Protos.{ ContainerInfo, CommandInfo }
import collection.immutable.Seq

/**
  * VolumeProvider is an interface implemented by storage volume providers
  */
trait VolumeProvider[+T <: Volume] {
  /** appValidation implements a provider's app validation rules */
  val appValidation: Validator[AppDefinition]
  /** groupValidation implements a provider's group validation rules */
  val groupValidation: Validator[Group]

  /** build adds v to the given builder **/
  def build(builder: ContainerInfo.Builder, v: Volume): Unit
}

trait ExternalVolumeProvider extends VolumeProvider[ExternalVolume] {
  val name: String

  /**
    * see implicit validator in the ExternalVolume class for reference.
    */
  val volumeValidation: Validator[ExternalVolume]

  /** build adds ev to the given builder **/
  def build(containerType: ContainerInfo.Type, builder: CommandInfo.Builder, ev: ExternalVolume): Unit
}

trait ExternalVolumeProviderRegistry {
  /**
    * @return the ExternalVolumeProvider interface registered for the given name; if name is None then
    * the default PersistenVolumeProvider implementation is returned. None is returned if Some name is given
    * but no volume provider is registered for that name.
    */
  def get(name: String): Option[ExternalVolumeProvider]

  def all: Iterable[ExternalVolumeProvider]
}

/**
  * API facade for callers interested in storage volumes
  */
object VolumesModule {
  lazy val providers: ExternalVolumeProviderRegistry = StaticExternalVolumeProviderRegistry

  def build(builder: ContainerInfo.Builder, v: ExternalVolume): Unit = {
    providers.get(v.external.providerName).foreach { _.build(builder, v) }
  }

  def build(containerType: ContainerInfo.Type, builder: CommandInfo.Builder, v: ExternalVolume): Unit = {
    providers.get(v.external.providerName).foreach { _.build(containerType, builder, v) }
  }

  def validExternalVolume: Validator[ExternalVolume] = new Validator[ExternalVolume] {
    def apply(ev: ExternalVolume) = providers.get(ev.external.providerName) match {
      case Some(p) => p.volumeValidation(ev)
      case None    => Failure(Set(RuleViolation(None, "is unknown provider", Some("external/providerName"))))
    }
  }

  /** @return a validator that checks the validity of a container given the related volume providers */
  def validApp(): Validator[AppDefinition] = new Validator[AppDefinition] {
    def apply(app: AppDefinition) = providers.all.map(_.appValidation).map(validate(app)(_)).fold(Success)(_ and _)
  }

  /** @return a validator that checks the validity of a group given the related volume providers */
  def validGroup(): Validator[Group] = new Validator[Group] {
    def apply(grp: Group) = providers.all.map(_.groupValidation).map(validate(grp)(_)).fold(Success)(_ and _)
  }
}
